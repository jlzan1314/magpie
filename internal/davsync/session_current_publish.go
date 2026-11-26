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
	Checks  map[string]string
	Changes map[string]currentChange
	Failed  []sessions.BatchFailure
	Skipped int
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

func (s batchStore) prepareCurrent(ctx context.Context, head sessionLibrary, refs []sessions.BatchSource, all, outgoing bool, extra []sessions.BatchConversation) (currentPlan, error) {
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
	mergingBackup := false
	visit := func(c sessions.BatchConversation, mark string) error {
		key := currentThreadKey(c.Sources[0])
		old := head.Threads[key]
		raw, err := json.Marshal(c)
		if err != nil {
			return err
		}
		revision := batchDigest(raw)
		if mergingBackup {
			previous := old
			if change, ok := plan.Changes[key]; ok {
				previous = change.Thread
			}
			if previous.Count > 0 && previous.Revision != revision {
				compatible := previous.Count <= len(c.Parts) && fingerprintPrefix(c, previous.Count) == previous.Fingerprint
				if !compatible && previous.Count >= len(c.Parts) {
					kept, e := s.readThread(ctx, previous)
					if e != nil {
						return e
					}
					if slices.Equal(kept.Parts[:len(c.Parts)], c.Parts) {
						if _, pending := plan.Changes[key]; !pending {
							plan.Checks[key] = previous.Revision
						}
						return nil
					}
				}
				if !compatible {
					key = batchDigest([]byte(key + "\x00" + c.Fingerprint()))
					old = head.Threads[key]
				}
			} else if previous.Revision == revision {
				if _, pending := plan.Changes[key]; !pending {
					plan.Checks[key] = revision
				}
				return nil
			}
		}
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
	plan.Failed, err = sessions.VisitBatch(ctx, refs, all, true, outgoing, skip, visit)
	if err != nil {
		return plan, err
	}
	mergingBackup = true
	for _, c := range extra {
		if err = visit(c, ""); err != nil {
			return plan, err
		}
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
	r := SessionBatchResult{Failed: slices.Clone(plan.Failed), Skipped: plan.Skipped}
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
	sizes, err := s.dav.listRemoteSizes(ctx, folder, sessionDAVDir, currentSessionDir)
	if err != nil {
		return r, err
	}
	objects := map[batchObject]bool{}
	for _, ch := range check {
		for _, p := range ch.Thread.Pieces {
			for _, o := range p.objects() {
				objects[o] = true
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
	verified := map[batchObject]bool{}
	ready := false
	changes := maps.Clone(plan.Changes)
	for _, key := range slices.Sorted(maps.Keys(check)) {
		t := check[key].Thread
		for _, p := range t.Pieces {
			for _, o := range p.objects() {
				if verified[o] {
					continue
				}
				name := s.remoteObjectPath(o.kind, o.id)[2]
				info, e := os.Stat(s.filename(o.kind, o.id))
				// An existing prefix from another device may only be on DAV.
				if errors.Is(e, fs.ErrNotExist) {
					e = s.verifyRemote(ctx, o.kind, o.id)
					if e != nil {
						return r, e
					}
					verified[o] = true
					r.Skipped++
					continue
				}
				if e != nil {
					return r, e
				}
				probe := sizes
				e = retrySessionOperation(ctx, func() error {
					skipped, e := s.publishObject(ctx, o, info.Size(), probe, batchStore{}, nil, &ready)
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
				verified[o] = true
				delete(sizes, name) // never rely on stale pre-write metadata
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
	if len(changes) == 0 && len(plan.Checks) == 0 && len(r.Failed) > 0 && plan.Skipped == 0 {
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
		for key, ch := range changes {
			old := head.Threads[key]
			if old.Revision == ch.Thread.Revision {
				continue
			}
			if old.Revision != ch.Base {
				return r, errChanged
			}
			head.Threads[key] = ch.Thread
			dirty = true
		}
		if len(head.Threads) == 0 {
			return r, errEmptySessionBatch
		}
		if dirty {
			if !ready {
				for _, dirs := range [][]string{{folder}, {folder, sessionDAVDir}, {folder, sessionDAVDir, currentSessionDir}} {
					if err = s.dav.ensureSessionCollection(ctx, dirs); err != nil {
						return r, err
					}
				}
				ready = true
			}
			if tag == "" {
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
			for key, ch := range changes {
				if observed.Threads[key].Revision != ch.Thread.Revision {
					return r, errChanged
				}
			}
			r.Uploaded++
		} else {
			r.Skipped++
		}
		marks := s.marks()
		for key, ch := range changes {
			marks[key] = currentMark{ch.Mark, ch.Thread.Revision}
		}
		raw, _ := json.Marshal(marks)
		if err = writeBatchFile(s.checkpoint(), raw); err != nil {
			return r, err
		}
		r.Backup, r.Remote, r.Conversations = currentSessionID, true, len(head.visible())
		r.Duplicates = len(head.Threads) - r.Conversations
		r.selected = map[string]currentThread{}
		for key := range changes {
			r.selected[key] = head.Threads[key]
		}
		for key, rev := range plan.Checks {
			if t := head.Threads[key]; t.Revision == rev {
				r.selected[key] = t
			}
		}
		updateSessionActivity(ctx, func(a *SessionActivity) { a.Objects = a.Count; a.Bytes = a.Total; a.Sending = 0 })
		return r, nil
	}
	return r, errChanged
}

func (s batchStore) updateCurrent(ctx context.Context, req SessionBatchRequest, extra []sessions.BatchConversation, onPrepared func(string, []sessions.BatchFailure) error) (SessionBatchResult, error) {
	head, _, err := s.readHead(ctx)
	if err != nil {
		return SessionBatchResult{}, err
	}
	plan, err := s.prepareCurrent(ctx, head, req.Sources, req.All, onPrepared != nil, extra)
	if err != nil {
		return SessionBatchResult{}, err
	}
	if len(plan.Changes) == 0 && len(plan.Checks) > 0 {
		return s.commitPlan(ctx, plan)
	}
	if len(plan.Changes) == 0 {
		r := SessionBatchResult{Skipped: plan.Skipped, Failed: plan.Failed, Conversations: len(head.Threads)}
		if plan.Skipped > 0 {
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
