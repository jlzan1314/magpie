package davsync

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/yetone/magpie/internal/backup"
	"github.com/yetone/magpie/internal/filelock"
	"github.com/yetone/magpie/internal/sessions"
)

func (s batchStore) stagePiece(ctx context.Context, c sessions.BatchConversation) (currentPiece, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return currentPiece{}, err
	}
	p := currentPiece{ID: batchDigest(raw)}
	if len(raw) <= batchPartSize {
		_, err = s.keepLocal("conversations", p.ID, raw)
		return p, err
	}
	tmp, err := os.MkdirTemp("", "magpie-current-parts-")
	if err != nil {
		return p, err
	}
	defer os.RemoveAll(tmp)
	parts, err := prepareBatchParts(ctx, tmp, s.passphrase, batchPartSize, raw)
	if err != nil {
		return p, err
	}
	if len(parts) == 1 {
		return p, s.keepPrepared(tmp, parts[0], batchObject{"conversations", p.ID})
	}
	p.Parts = parts
	for _, id := range parts {
		if err = s.keepPrepared(tmp, id, batchObject{"parts", id}); err != nil {
			return p, err
		}
	}
	return p, nil
}

type currentChange struct {
	Base   string
	Thread currentThread
	Mark   string
}
type currentPlan struct {
	Checks     map[string]string
	Changes    map[string]currentChange
	Failed     []sessions.BatchFailure
	Duplicates int  `json:",omitempty"`
	Unified    bool `json:",omitempty"`
}

func (s batchStore) checkpoint() string { return filepath.Join(s.localRoot, "marks.json") }

type currentMark struct{ Mark, Revision string }

func (s batchStore) marks() map[string]currentMark {
	out := map[string]currentMark{}
	raw, err := os.ReadFile(s.checkpoint())
	if err == nil {
		if json.Unmarshal(raw, &out) != nil || out == nil {
			return map[string]currentMark{}
		}
	}
	return out
}

func (s batchStore) prepareLocalArchive(ctx context.Context, head sessionLibrary) (currentPlan, error) {
	plan := currentPlan{Changes: map[string]currentChange{}, Checks: map[string]string{}}
	origin, err := sessionDeviceID()
	if err != nil {
		return plan, err
	}
	host, _ := os.Hostname()
	marks := s.marks()
	skip := func(ref sessions.BatchSource, stamp string) bool {
		key := currentThreadKey(ref)
		t, exists := head.Threads[key]
		if exists && stamp != "" && marks[key] == (currentMark{stamp, t.Revision}) {
			plan.Checks[key] = t.Revision
			return true
		}
		return false
	}
	visit := func(c sessions.BatchConversation, mark string) error {
		key := currentThreadKey(c.Sources[0])
		old := head.Threads[key]
		raw, err := json.Marshal(c)
		if err != nil {
			return err
		}
		revision := batchDigest(raw)
		if revision == old.Revision {
			marks[key] = currentMark{mark, revision}
			plan.Checks[key] = revision
			return nil
		}
		t := currentThread{Meta: c, Origin: origin, Device: host, Revision: revision, Fingerprint: c.Fingerprint(), Count: len(c.Parts)}
		t.Meta.Parts = nil
		start := 0
		if old.Count > 0 && old.Count <= len(c.Parts) && fingerprintPrefix(c, old.Count) == old.Fingerprint {
			start = old.Count
			t.Pieces = slices.Clone(old.Pieces)
		}
		if start < len(c.Parts) {
			delta := sessions.BatchConversation{Parts: c.Parts[start:]}
			piece, err := s.stagePiece(ctx, delta)
			if err != nil {
				return err
			}
			t.Pieces = append(t.Pieces, piece)
		}
		plan.Changes[key] = currentChange{old.Revision, t, mark}
		return nil
	}
	plan.Failed, err = sessions.VisitBatch(ctx, nil, true, true, false, skip, visit)
	if err != nil {
		return plan, err
	}
	if data, e := json.Marshal(marks); e == nil {
		_ = writeBatchFile(s.checkpoint(), data)
	}
	return plan, nil
}
func fingerprintPrefix(c sessions.BatchConversation, n int) string {
	c.Parts = c.Parts[:n]
	return c.Fingerprint()
}

func preferCurrentThread(candidate, old currentThread) bool {
	if old.Revision == "" {
		return true
	}
	if order := candidate.Meta.Last.Compare(old.Meta.Last); order != 0 {
		return order > 0
	}
	if candidate.Count != old.Count {
		return candidate.Count > old.Count
	}
	return strings.Compare(candidate.Fingerprint, old.Fingerprint) > 0
}

func rewritesOwnSession(candidate, old currentThread) bool {
	return old.Revision != "" && candidate.Origin == old.Origin && candidate.Meta.Sources[0] == old.Meta.Sources[0] &&
		candidate.Fingerprint != old.Fingerprint && !candidate.Meta.Last.Before(old.Meta.Last)
}

func (s batchStore) prepareUnified(ctx context.Context, head sessionLibrary, req SessionBatchRequest, extra []sessions.BatchConversation) (currentPlan, error) {
	plan := currentPlan{Changes: map[string]currentChange{}, Checks: map[string]string{}, Unified: true}
	origin, err := sessionDeviceID()
	if err != nil {
		return plan, err
	}
	host, _ := os.Hostname()
	marks := s.marks()
	local := map[sessions.BatchSource]string{}
	for key, t := range head.Threads {
		if t.Origin == origin {
			local[t.Meta.Sources[0]] = key
		}
	}
	skip := func(ref sessions.BatchSource, stamp string) bool {
		key, ok := local[ref]
		t := head.Threads[key]
		if !ok || stamp == "" || marks[key] != (currentMark{stamp, t.Revision}) {
			return false
		}
		plan.Checks[key] = t.Revision
		return true
	}
	visited := map[string]bool{}
	localCapture := true
	visit := func(c sessions.BatchConversation, mark string) error {
		raw, err := json.Marshal(c)
		if err != nil {
			return err
		}
		candidate := currentThread{Meta: c, Origin: origin, Device: host, Count: len(c.Parts), Revision: batchDigest(raw), Fingerprint: c.Fingerprint()}
		key := candidate.identity()
		if visited[key] {
			plan.Duplicates++
		}
		visited[key] = true
		base := head.Threads[key]
		old := base
		pending, changed := plan.Changes[key]
		if changed {
			old = pending.Thread
		}
		if !preferCurrentThread(candidate, old) && !(localCapture && rewritesOwnSession(candidate, old)) {
			if !changed {
				plan.Checks[key] = old.Revision
			}
			return nil
		}
		start := 0
		if base.Count > 0 && base.Count <= candidate.Count && fingerprintPrefix(c, base.Count) == base.Fingerprint {
			start = base.Count
			candidate.Pieces = slices.Clone(base.Pieces)
		}
		if start < candidate.Count {
			p, err := s.stagePiece(ctx, sessions.BatchConversation{Parts: c.Parts[start:]})
			if err != nil {
				return err
			}
			p.Collection = key
			candidate.Pieces = append(candidate.Pieces, p)
		}
		candidate.Meta.Parts = nil
		plan.Changes[key] = currentChange{base.Revision, candidate, mark}
		delete(plan.Checks, key)
		return nil
	}
	plan.Failed, err = sessions.VisitBatch(ctx, req.Sources, req.All, true, len(head.Threads) != 0, skip, visit)
	if err != nil {
		return plan, err
	}
	localCapture = false
	for _, entry := range extra {
		if err := visit(entry, ""); err != nil {
			return plan, err
		}
	}
	return plan, nil
}

func (s batchStore) moveLegacyPieces(ctx context.Context, plan *currentPlan, legacy sessionLibrary) error {
	for key, thread := range legacy.Threads {
		if _, ok := plan.Changes[key]; !ok {
			plan.Changes[key] = currentChange{Thread: thread}
		}
		delete(plan.Checks, key)
	}
	for key, change := range plan.Changes {
		change.Thread.Pieces = slices.Clone(change.Thread.Pieces)
		for i, p := range change.Thread.Pieces {
			if p.Collection != "" {
				continue
			}
			for _, o := range p.objects() {
				_, err := readVerifiedBatchFile(ctx, &s, o.kind, o.id, func(raw []byte) (struct{}, error) {
					return struct{}{}, s.verifyObject(raw, o.kind, o.id)
				})
				if err != nil {
					return err
				}
			}
			p.Collection = key
			change.Thread.Pieces[i] = p
		}
		plan.Changes[key] = change
	}
	return nil
}

func (s batchStore) savePlan(plan currentPlan) (string, error) {
	raw, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	id := batchDigest(raw)
	sealed, err := backup.SealData(raw, s.passphrase)
	if err != nil {
		return "", err
	}
	return id, writeBatchFile(filepath.Join(s.localRoot, "pending", id+".sealed"), sealed)
}
func (s batchStore) readPlan(id string) (currentPlan, error) {
	var plan currentPlan
	if !validBatchID(id) {
		return plan, errors.New("invalid prepared backup ID")
	}
	raw, err := os.ReadFile(filepath.Join(s.localRoot, "pending", id+".sealed"))
	if err != nil {
		return plan, err
	}
	raw, err = backup.OpenData(raw, s.passphrase)
	if err != nil {
		return plan, err
	}
	if batchDigest(raw) != id {
		return plan, errors.New("damaged prepared session update")
	}
	err = json.Unmarshal(raw, &plan)
	return plan, err
}

func (s batchStore) commitPlan(ctx context.Context, plan currentPlan) (SessionBatchResult, error) {
	r := SessionBatchResult{Failed: slices.Clone(plan.Failed), Duplicates: plan.Duplicates}
	initial, _, err := s.readHead(ctx)
	if err != nil {
		return r, err
	}
	check := maps.Clone(plan.Changes)
	for key, rev := range plan.Checks {
		if t, ok := initial.Threads[key]; ok && t.Revision == rev {
			check[key] = currentChange{Base: rev, Thread: t}
		}
	}
	type storedObject struct {
		batchObject
		collection string
	}
	objects := map[storedObject]bool{}
	for _, ch := range check {
		for _, p := range ch.Thread.Pieces {
			for _, o := range p.objects() {
				objects[storedObject{o, p.Collection}] = true
			}
		}
	}
	var total int64
	for o := range objects {
		if f, e := os.Stat(s.filename(o.kind, o.id)); e == nil {
			total += f.Size()
		}
	}
	updateSessionActivity(ctx, func(a *SessionActivity) { a.Phase = "uploading"; a.Count = len(objects) + 1; a.Total = total })
	verified := map[storedObject]bool{}
	type remoteCollection struct {
		sizes map[string]remoteSessionFile
		ready bool
	}
	collections := map[string]*remoteCollection{}
	changes := maps.Clone(plan.Changes)
	for _, key := range slices.Sorted(maps.Keys(check)) {
		t := check[key].Thread
		for _, p := range t.Pieces {
			for _, o := range p.objects() {
				stored := storedObject{o, p.Collection}
				if verified[stored] {
					continue
				}
				pieceStore := s.inThread(p.Collection)
				objectPath := pieceStore.remoteObjectPath(o.kind, o.id)
				name := objectPath[len(objectPath)-1]
				info, e := os.Stat(s.filename(o.kind, o.id))
				// An existing prefix from another device may only be on DAV.
				if errors.Is(e, fs.ErrNotExist) {
					e = pieceStore.verifyRemote(ctx, o.kind, o.id)
					if e != nil {
						return r, e
					}
					verified[stored] = true
					r.Skipped++
					continue
				}
				if e != nil {
					return r, e
				}
				collection := collections[p.Collection]
				if collection == nil {
					sizes, err := s.dav.listRemoteSizes(ctx, append([]string{folder}, objectPath[:len(objectPath)-1]...)...)
					if err != nil {
						return r, err
					}
					collection = &remoteCollection{sizes: sizes}
					collections[p.Collection] = collection
				}
				probe := collection.sizes
				e = retrySessionOperation(ctx, func() error {
					skipped, e := pieceStore.publishObject(ctx, o, info.Size(), probe, batchStore{}, nil, &collection.ready)
					probe = nil
					if e == nil {
						if skipped {
							r.Skipped++
						} else {
							r.Uploaded++
						}
					}
					return e
				})
				if e != nil {
					var rejected *sessionObjectRejected
					if !errors.As(e, &rejected) {
						return r, e
					}
					for _, ref := range t.Meta.Sources {
						r.Failed = append(r.Failed, sessions.BatchFailure{Source: ref, Title: t.Meta.Title, Error: e.Error()})
					}
					delete(changes, key)
					break
				}
				verified[stored] = true
				delete(collection.sizes, name) // never rely on stale pre-write metadata
				updateSessionActivity(ctx, func(a *SessionActivity) { a.Objects++; a.Bytes += info.Size(); a.Sending = 0 })
			}
			if _, rejected := plan.Changes[key]; rejected {
				if _, ok := changes[key]; !ok {
					break
				}
			}
			if ctx.Err() != nil {
				break
			}
		}
	}
	if len(changes) == 0 && len(plan.Checks) == 0 && len(r.Failed) > 0 {
		return r, errAllSessionsRejected
	}
	// A desktop and CLI on this machine may submit edits concurrently. Keep
	// conditional creation safe even on servers that ignore If-None-Match.
	unlock, held := filelock.Lock(filepath.Join(s.localRoot, "publish.lock"), filelock.Wait)
	if !held {
		return r, errors.New("another session update is being committed")
	}
	defer unlock()
	// Re-read and merge after object upload. Conditional writes protect
	// simultaneous devices; a lost acknowledgement is recognized on replay.
	for attempt := 0; attempt < 8; attempt++ {
		var head sessionLibrary
		var tag string
		err = retrySessionOperation(ctx, func() error { var e error; head, tag, e = s.readHead(ctx); return e })
		if err != nil {
			return r, err
		}
		dirty := false
		written := map[string]string{}
		for key, ch := range changes {
			old := head.Threads[key]
			if old.Revision == ch.Thread.Revision {
				continue
			}
			if !preferCurrentThread(ch.Thread, old) && !(old.Revision == ch.Base && rewritesOwnSession(ch.Thread, old)) {
				continue
			}
			head.Threads[key] = ch.Thread
			written[key] = ch.Thread.Revision
			dirty = true
		}
		if len(head.Threads) == 0 {
			return r, errEmptySessionBatch
		}
		if dirty {
			if tag == "" {
				for _, dirs := range [][]string{{folder}, {folder, sessionDAVDir}, {folder, sessionDAVDir, currentSessionDir}} {
					if err = s.dav.ensureSessionCollection(ctx, dirs); err != nil {
						return r, err
					}
				}
				if err = s.dav.ensureSessionCollection(ctx, []string{folder, sessionDAVDir, currentSessionDir, "heads"}); err != nil {
					return r, err
				}
			}
			err = retrySessionOperation(ctx, func() error { return s.writeHead(ctx, head, tag) })
			if errors.Is(err, errChanged) {
				continue
			}
			if err != nil {
				return r, err
			}
			observed, _, err := s.readHead(ctx)
			if err != nil {
				return r, err
			}
			for key, revision := range written {
				if t := observed.Threads[key]; t.Revision != revision && !preferCurrentThread(t, head.Threads[key]) {
					return r, errChanged
				}
			}
			head = observed
			r.Uploaded++
		} else {
			r.Skipped++
		}
		marks := s.marks()
		for key, ch := range changes {
			if head.Threads[key].Revision == ch.Thread.Revision {
				marks[key] = currentMark{ch.Mark, ch.Thread.Revision}
			}
		}
		raw, _ := json.Marshal(marks)
		if err = writeBatchFile(s.checkpoint(), raw); err != nil {
			return r, err
		}
		r.Backup, r.Remote, r.Conversations = currentSessionID, true, len(head.visible())
		r.selected = map[string]currentThread{}
		for key := range changes {
			r.selected[key] = head.Threads[key]
		}
		for key := range plan.Checks {
			if t, ok := head.Threads[key]; ok {
				r.selected[key] = t
			}
		}
		updateSessionActivity(ctx, func(a *SessionActivity) { a.Objects = a.Count; a.Bytes = a.Total; a.Sending = 0 })
		return r, nil
	}
	return r, errChanged
}

func (s batchStore) updateCurrent(ctx context.Context, req SessionBatchRequest, extra []sessions.BatchConversation, onPrepared func(string, []sessions.BatchFailure) error) (SessionBatchResult, error) {
	head, tag, err := s.readHead(ctx)
	if err != nil {
		return SessionBatchResult{}, err
	}
	if tag == "" {
		head, err = s.readLegacyLibrary(ctx, true)
		if err != nil {
			return SessionBatchResult{}, err
		}
	}
	plan, err := s.prepareUnified(ctx, head, req, extra)
	if err != nil {
		return SessionBatchResult{}, err
	}
	if tag == "" {
		if err := s.moveLegacyPieces(ctx, &plan, head); err != nil {
			return SessionBatchResult{}, err
		}
	}
	if len(plan.Changes) == 0 && len(plan.Checks) > 0 {
		return s.commitPlan(ctx, plan)
	}
	if len(plan.Changes) == 0 {
		r := SessionBatchResult{Failed: plan.Failed, Conversations: len(head.Threads)}
		if len(head.Threads) > 0 && len(plan.Failed) == 0 {
			r.Backup, r.Remote = currentSessionID, true
			return r, nil
		}
		if len(plan.Failed) > 0 {
			return r, errors.New(plan.Failed[0].Error)
		}
		return r, errEmptySessionBatch
	}
	id, err := s.savePlan(plan)
	if err != nil {
		return SessionBatchResult{}, err
	}
	if onPrepared != nil {
		if err = onPrepared(id, plan.Failed); err != nil {
			return SessionBatchResult{}, err
		}
	}
	r, err := s.commitPlan(ctx, plan)
	if onPrepared == nil || err == nil {
		_ = os.Remove(filepath.Join(s.localRoot, "pending", id+".sealed"))
	}
	return r, err
}
