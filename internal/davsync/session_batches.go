package davsync

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/yetone/magpie/internal/backup"
	"github.com/yetone/magpie/internal/sessions"
	"github.com/yetone/magpie/internal/settings"
	"github.com/yetone/magpie/internal/steady"
)

const batchDir = "session-batches-v2"
const legacyBatchDir = "session-batches-v1"
const sessionDAVDir = "sessions"
const batchPackedLimit = 64 << 20

var errBatchPackedTooLarge = errors.New("packed session backup exceeds 64 MiB")

type batchGzipBuffer struct{ bytes.Buffer }

func (b *batchGzipBuffer) Write(p []byte) (int, error) {
	// The encrypted envelope is at least as large as the gzip body.
	if len(p) > batchPackedLimit-b.Len() {
		return 0, errBatchPackedTooLarge
	}
	return b.Buffer.Write(p)
}

type SessionBatchRequest struct {
	Current bool                   `json:"current,omitempty"`
	Sources []sessions.BatchSource `json:"sources"`
	All     bool                   `json:"all"`
	Entry   string                 `json:"entry,omitempty"`
	Backups []string               `json:"backups"`
	Targets []string               `json:"targets"`
	Cwd     string                 `json:"cwd"`
}

type SessionBatchResult struct {
	selected      map[string]currentThread // revisions actually committed for an immediate import
	Backup        string                   `json:"backup,omitempty"`
	Remote        bool                     `json:"remote"`
	Conversations int                      `json:"conversations"`
	Duplicates    int                      `json:"duplicates"`
	Uploaded      int                      `json:"uploaded"`
	Skipped       int                      `json:"skipped"`
	Failed        []sessions.BatchFailure  `json:"failed,omitempty"`
	Imports       []sessions.BatchImport   `json:"imports,omitempty"`
}

type batchEntryInfo struct {
	Title   string                 `json:"title,omitempty"`
	Sources []sessions.BatchSource `json:"sources,omitempty"`
}

type batchManifest struct {
	Origin  string              `json:"origin,omitempty"`
	Device  string              `json:"device,omitempty"`
	Info    []batchEntryInfo    `json:"info,omitempty"`
	Version int                 `json:"version"`
	Entries []string            `json:"entries"`
	Parts   map[string][]string `json:"parts,omitempty"`
}

type batchStore struct {
	headKey    string // shared catalog or a legacy device manifest
	threadKey  string
	current    bool // stable shared DAV collection
	localRoot  string
	archive    fs.FS
	dav        *dav
	passphrase string
	batchID    string
	remoteDir  string
	onPrepared func(string) error
}

// RunSessionBatch captures selected local sessions and optional earlier
// batches, merges them, publishes the encrypted backup and then imports
// the result. Importing a single existing backup works on a DAV follower.
func RunSessionBatch(ctx context.Context, req SessionBatchRequest) (SessionBatchResult, error) {
	c, ok := Load()
	if !ok || c.S3() || c.GitHub() {
		return SessionBatchResult{}, errors.New("set up WebDAV sync before backing up sessions")
	}
	return runSessionBatch(ctx, c, req, nil)
}

var errEmptySessionBatch = errors.New("select sessions or a session backup")

func runSessionBatch(ctx context.Context, c Config, req SessionBatchRequest, onPrepared func(string, []sessions.BatchFailure) error) (SessionBatchResult, error) {
	ctx = withSessionDAVTimeouts(ctx)
	var result SessionBatchResult
	d, err := newDAV(c)
	if err != nil {
		return result, err
	}
	store := batchStore{dav: d, passphrase: c.Passphrase}
	if req.Current {
		if len(req.Targets) == 0 {
			return result, errors.New("choose one or more target agents")
		}
		req.All = !c.FollowServer
		req.Backups = []string{currentSessionID}
	}
	if err := sessions.CheckBatchTargets(req.Targets, strings.TrimSpace(req.Cwd)); err != nil {
		return result, err
	}
	if req.Entry != "" && (len(req.Backups) != 1 || req.All || len(req.Sources) > 0 || !validBatchID(req.Entry)) {
		return result, errors.New("choose one backup to restore a single conversation")
	}
	build := req.All || len(req.Sources) > 0 || len(req.Backups) > 1
	loadBackup := func(name, entry string) ([]sessions.BatchConversation, error) {
		if name == currentSessionID {
			return currentStore(c, d).loadCurrent(ctx, entry, req.Current)
		}
		return store.loadEntries(ctx, name, entry)
	}
	if build && !c.FollowServer {
		var extra []sessions.BatchConversation
		for _, id := range req.Backups {
			if id == currentSessionID {
				continue
			}
			entries, e := loadBackup(strings.TrimSpace(id), req.Entry)
			if e != nil {
				return result, e
			}
			extra = append(extra, entries...)
		}
		cur := currentStore(c, d)
		// Manual requests and the scheduler can overlap in separate processes.
		// Staging files are content addressed; the commit uses compare-and-swap.
		result, err = cur.updateCurrent(ctx, req, extra, onPrepared)
		if result.Remote && onPrepared == nil {
			_ = writeBatchFile(batchHint(c), []byte(currentSessionID))
		}
		if err != nil || len(req.Targets) == 0 {
			return result, err
		}
		var entries []sessions.BatchConversation
		if slices.Contains(req.Backups, currentSessionID) {
			entries, err = cur.loadCurrent(ctx, "", req.Current)
			if err != nil {
				return result, err
			}
		} else {
			for _, k := range slices.Sorted(maps.Keys(result.selected)) {
				entry, e := cur.readThread(ctx, result.selected[k])
				if e != nil {
					return result, e
				}
				entries = append(entries, entry)
			}
		}
		result.Imports, err = sessions.ApplyBatch(ctx, entries, req.Targets, strings.TrimSpace(req.Cwd))
		return result, err
	}
	collect := sessions.CollectAvailableBatch
	if onPrepared != nil {
		collect = sessions.CollectOutgoingBatch
	}
	entries, failures, err := collect(ctx, req.Sources, req.All)
	result.Failed = failures
	if onPrepared != nil {
		store.onPrepared = func(id string) error { return onPrepared(id, failures) }
	}
	if err != nil {
		return result, err
	}
	for _, name := range req.Backups {
		old, err := loadBackup(strings.TrimSpace(name), req.Entry)
		if err != nil {
			return result, err
		}
		entries = append(entries, old...)
	}
	if len(entries) == 0 {
		if len(result.Failed) != 0 {
			return result, errors.New(result.Failed[0].Error)
		}
		return result, errEmptySessionBatch
	}
	merged := sessions.MergeBatch(entries)
	if !build && len(req.Backups) == 1 && req.Backups[0] == currentSessionID {
		merged = entries
	}
	result.Conversations = len(merged)
	result.Duplicates = len(entries) - len(merged)
	if build {
		var stats batchPublishStats
		result.Backup, stats, err = store.save(ctx, merged, !c.FollowServer, batchPartSize)
		result.Remote = err == nil && !c.FollowServer
		result.Uploaded, result.Skipped = stats.uploaded, stats.skipped
		result.Failed = append(result.Failed, stats.failed...)
		if len(stats.failed) > 0 {
			result.Conversations = stats.conversations
		}
		if err != nil {
			// A local backup ID is still useful after a network failure.
			return result, err
		}
	} else {
		result.Backup = strings.TrimSpace(req.Backups[0])
	}
	if len(req.Targets) != 0 {
		if len(result.Failed) > 0 && result.Remote {
			merged, err = store.load(ctx, result.Backup)
			if err != nil {
				return result, err
			}
		}
		if !build && req.Backups[0] == currentSessionID {
			result.Imports, err = sessions.ApplyBatch(ctx, merged, req.Targets, strings.TrimSpace(req.Cwd))
		} else {
			result.Imports, err = sessions.ImportBatch(ctx, merged, req.Targets, strings.TrimSpace(req.Cwd))
		}
	}
	return result, err
}

type batchPublishStats struct {
	failed        []sessions.BatchFailure
	conversations int
	uploaded      int
	skipped       int
}

func (s batchStore) save(ctx context.Context, entries []sessions.BatchConversation, publish bool, partSize int) (string, batchPublishStats, error) {
	var stats batchPublishStats
	device, _ := os.Hostname()
	origin, err := sessionDeviceID()
	if err != nil {
		return "", stats, err
	}
	manifest := batchManifest{Version: 2, Device: device, Origin: origin}
	prepared := make(map[string][]string)
	staging := ""
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return "", stats, err
		}
		data, err := json.Marshal(entry)
		if err != nil {
			return "", stats, err
		}
		id := batchDigest(data)
		manifest.Entries = append(manifest.Entries, id)
		manifest.Info = append(manifest.Info, batchEntryInfo{entry.Title, entry.Sources})
		if len(data) <= partSize {
			continue
		}
		if staging == "" {
			staging, err = os.MkdirTemp("", "magpie-session-parts-")
			if err != nil {
				return "", stats, err
			}
			defer os.RemoveAll(staging)
		}
		parts, err := prepareBatchParts(ctx, staging, s.passphrase, partSize, data)
		if err != nil {
			return "", stats, err
		}
		prepared[id] = parts
		if len(parts) > 1 {
			if manifest.Parts == nil {
				manifest.Parts = make(map[string][]string)
			}
			manifest.Version = 3
			manifest.Parts[id] = parts
		}
	}
	plain, err := json.Marshal(manifest)
	if err != nil {
		return "", stats, err
	}
	s.batchID = batchDigest(plain)
	var objects []batchObject
	seenParts := make(map[string]bool)
	for i, entry := range entries {
		if err := ctx.Err(); err != nil {
			return "", stats, err
		}
		id := manifest.Entries[i]
		parts := prepared[id]
		if len(parts) > 1 {
			for _, part := range parts {
				if seenParts[part] {
					continue
				}
				seenParts[part] = true
				object := batchObject{"parts", part}
				if err := s.keepPrepared(staging, part, object); err != nil {
					return "", stats, err
				}
				objects = append(objects, object)
			}
			continue
		}
		object := batchObject{"conversations", id}
		objects = append(objects, object)
		if len(parts) == 1 {
			if err := s.keepPrepared(staging, parts[0], object); err != nil {
				return "", stats, err
			}
			continue
		}
		data, err := json.Marshal(entry)
		if err != nil {
			return "", stats, err
		}
		if _, err := s.keepLocal(object.kind, object.id, data); err != nil {
			return "", stats, err
		}
	}
	if _, err := s.keepLocal("backups", s.batchID, plain); err != nil {
		return "", stats, err
	}
	if !publish {
		return s.batchID, stats, nil
	}
	if s.onPrepared != nil {
		if err := s.onPrepared(s.batchID); err != nil {
			return s.batchID, stats, err
		}
	}
	return s.publish(ctx, objects, manifest)
}

// publish sends the prepared files and then the manifest. It is also the
// resume path, where the manifest on disk supplies the same objects.
func batchHint(c Config) string { return filepath.Join(settings.Dir(), batchDir, "last-"+stateKey(c)) }

func (s batchStore) publish(ctx context.Context, objects []batchObject, manifest batchManifest) (string, batchPublishStats, error) {
	return s.publishFrom(ctx, objects, manifest, nil)
}

func (s batchStore) publishFrom(ctx context.Context, objects []batchObject, manifest batchManifest, reuse *batchStore) (string, batchPublishStats, error) {
	var stats batchPublishStats
	hint := batchHint(Config{URL: s.dav.base.String(), User: s.dav.user, Passphrase: s.passphrase})
	last, _ := os.ReadFile(hint)
	previous := s
	previous.batchID = string(last)
	if reuse != nil {
		previous = *reuse
	}
	var sizes, previousSizes map[string]remoteSessionFile
	err := retrySessionOperation(ctx, func() error {
		var err error
		s, sizes, err = s.locateRemoteBatch(ctx)
		if err != nil {
			return err
		}
		if validBatchID(previous.batchID) && previous.batchID != s.batchID {
			previous.remoteDir = ""
			previous, previousSizes, err = previous.locateRemoteBatch(ctx)
		}
		return err
	})
	if err != nil {
		return s.batchID, stats, err
	}
	objects = append(objects, batchObject{"backups", s.batchID})
	var total int64
	for _, o := range objects {
		if info, err := os.Stat(s.filename(o.kind, o.id)); err == nil {
			total += info.Size()
		}
	}
	updateSessionActivity(ctx, func(a *SessionActivity) {
		a.Phase = "uploading"
		a.Count = len(objects)
		a.Objects = 0
		a.Total = total
		a.Bytes = 0
	})
	ready := false
	rejected := map[batchObject]error{}
	for _, object := range objects {
		if object.kind == "backups" && len(rejected) > 0 {
			return s.publishPartial(ctx, manifest, rejected)
		}
		file := s.filename(object.kind, object.id)
		info, err := os.Stat(file)
		if err != nil {
			if object.kind != "backups" && len(manifest.Info) == len(manifest.Entries) && errors.Is(err, fs.ErrNotExist) {
				rejected[object] = err
				continue
			}
			return s.batchID, stats, err
		}
		var skipped bool
		probe := sizes
		err = retrySessionOperation(ctx, func() error {
			var err error
			skipped, err = s.publishObject(ctx, object, info.Size(), probe, previous, previousSizes, &ready)
			probe = nil // a lost response may have committed the object
			return err
		})
		if err != nil {
			var bad *sessionObjectRejected
			var temporary *temporarySessionHTTP
			if object.kind != "backups" && (errors.As(err, &bad) || errors.As(err, &temporary)) && len(manifest.Info) == len(manifest.Entries) {
				rejected[object] = err
				continue
			}
			return s.batchID, stats, err
		}
		if skipped {
			stats.skipped++
		} else {
			stats.uploaded++
		}
		updateSessionActivity(ctx, func(a *SessionActivity) { a.Sending = 0; a.Objects++; a.Bytes += info.Size() })
	}
	if previous.batchID != s.batchID {
		_ = steady.WriteFile(hint, []byte(s.batchID), 0o600)
	}
	return s.batchID, stats, nil
}

func (s batchStore) publishObject(ctx context.Context, object batchObject, size int64, sizes map[string]remoteSessionFile, previous batchStore, previousSizes map[string]remoteSessionFile, ready *bool) (bool, error) {
	kind, id := object.kind, object.id
	target := s.url(kind, id)
	objectPath := s.remoteObjectPath(kind, id)
	name := objectPath[len(objectPath)-1]
	same, exists, err := s.dav.remoteSizeMatches(ctx, target, size, sizes, name)
	if err != nil {
		return false, err
	}
	if same {
		match, err := s.receiptMatches(ctx, kind, id, size, sizes[name].etag)
		if err != nil || match {
			return match, err
		}
	}
	if !*ready {
		dirs := append([]string{folder}, objectPath[:len(objectPath)-1]...)
		for n := 1; n <= len(dirs); n++ {
			if err := s.dav.ensureSessionCollection(ctx, dirs[:n]); err != nil {
				return false, err
			}
		}
		*ready = true
	}
	if !exists && previousSizes != nil && kind != "backups" {
		if n, found := previousSizes[previous.remoteObjectPath(kind, id)[2]]; found && n.size == size {
			copied, err := s.copyObject(ctx, previous, object, size)
			if err != nil || copied {
				return copied, err
			}
		}
	}
	raw, err := readBatchFile(s.filename(kind, id))
	if err != nil {
		return false, err
	}
	err = s.publishRaw(ctx, kind, id, raw, exists)
	if errors.Is(err, errNotModified) {
		return true, nil
	}
	return false, err
}

func (s batchStore) receiptFile(kind, id string) string {
	key := stateKey(Config{URL: s.url(kind, id), User: s.dav.user, Passphrase: s.passphrase})
	if s.current {
		return filepath.Join(s.localRoot, "verified", key)
	}
	return filepath.Join(settings.Dir(), batchDir, s.batchID, ".verified", key)
}

func (s batchStore) receiptMatches(ctx context.Context, kind, id string, size int64, listedTag string) (bool, error) {
	tag, err := os.ReadFile(s.receiptFile(kind, id))
	if err != nil || len(tag) == 0 {
		return false, nil
	}
	if listedTag != "" {
		return listedTag == string(tag), nil
	}
	res, err := s.dav.send(ctx, http.MethodHead, s.url(kind, id), nil, map[string]string{"Accept-Encoding": "identity"})
	if err != nil {
		return false, err
	}
	res.Body.Close()
	return res.StatusCode == http.StatusOK && res.Header.Get("ETag") == string(tag) && (res.ContentLength == size || res.ContentLength <= 0), nil
}

func (s batchStore) verifyResponse(kind, id string, raw []byte, tag string) error {
	if err := s.verifyObject(raw, kind, id); err != nil {
		return err
	}
	if len(tag) >= 2 && tag[0] == '"' && tag[len(tag)-1] == '"' {
		_ = writeBatchFile(s.receiptFile(kind, id), []byte(tag))
	}
	return nil
}

func (s batchStore) verifyRemote(ctx context.Context, kind, id string) error {
	res, err := s.dav.do(ctx, http.MethodGet, s.url(kind, id), nil, map[string]string{"Accept-Encoding": "identity"})
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("verifying session backup: HTTP %d", res.StatusCode)
	}
	raw, err := readBatchLimited(res.Body)
	if err != nil {
		return err
	}
	return s.verifyResponse(kind, id, raw, res.Header.Get("ETag"))
}

// A known backup stays in the collection where it was first uploaded.
// This keeps same-size skips and interrupted transfers valid after an upgrade.
func (s batchStore) locateRemoteBatch(ctx context.Context) (batchStore, map[string]remoteSessionFile, error) {
	if s.remoteDir != "" {
		sizes, err := s.dav.listRemoteSizes(ctx, folder, s.remoteDir, s.batchID)
		return s, sizes, err
	}
	s.remoteDir = sessionDAVDir
	sizes, found, err := s.remoteBatchContents(ctx)
	if err != nil || found {
		return s, sizes, err
	}
	legacy := s
	legacy.remoteDir = batchDir
	olderSizes, found, err := legacy.remoteBatchContents(ctx)
	if err != nil || found {
		return legacy, olderSizes, err
	}
	return s, sizes, nil
}

func (s batchStore) remoteBatchContents(ctx context.Context) (map[string]remoteSessionFile, bool, error) {
	sizes, err := s.dav.listRemoteSizes(ctx, folder, s.remoteDir, s.batchID)
	if err != nil || sizes != nil {
		return sizes, len(sizes) != 0, err
	}
	// PROPFIND is optional on some servers. A completed manifest is enough
	// to identify a backup on those servers.
	_, found, err := s.dav.remoteSize(ctx, s.url("backups", s.batchID))
	return nil, found, err
}

func (s batchStore) copyObject(ctx context.Context, previous batchStore, object batchObject, size int64) (bool, error) {
	target := s.url(object.kind, object.id)
	res, err := s.dav.send(ctx, "COPY", previous.url(object.kind, object.id), nil, map[string]string{"Destination": target, "Overwrite": "F"})
	if err != nil {
		return false, err
	}
	res.Body.Close()
	switch res.StatusCode {
	case http.StatusCreated, http.StatusNoContent, http.StatusOK, http.StatusPreconditionFailed:
		err := s.verifyRemote(ctx, object.kind, object.id)
		return err == nil, err
	case http.StatusNotFound, http.StatusGone, http.StatusConflict, http.StatusForbidden, http.StatusBadRequest, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return false, nil
	default:
		return false, fmt.Errorf("copying WebDAV conversation: HTTP %d", res.StatusCode)
	}
}

var errSessionBackupMissing = errors.New("session backup was not found")

func (m batchManifest) valid(version int) bool {
	validVersion := m.Version == version || version == 2 && m.Version == 3
	return validVersion && len(m.Entries) != 0 && (m.Version == 3) == (len(m.Parts) != 0)
}

// resumeSessionBatch publishes an already sealed local snapshot. Live Agent
// history may have changed while the earlier upload was interrupted.
func resumeSessionBatch(ctx context.Context, c Config, id, remoteDir string) (SessionBatchResult, error) {
	ctx = withSessionDAVTimeouts(ctx)
	var result SessionBatchResult
	if !validBatchID(id) {
		return result, errors.New("invalid pending session backup ID")
	}
	d, err := newDAV(c)
	if err != nil {
		return result, err
	}
	cur := currentStore(c, d)
	plan, e := cur.readPlan(id)
	if e == nil {
		if !plan.Unified {
			legacy, err := cur.readLibrary(ctx, true)
			if err != nil {
				return result, err
			}
			changes := map[string]currentChange{}
			for _, ch := range plan.Changes {
				key := ch.Thread.identity()
				if previous := changes[key].Thread; preferCurrentThread(ch.Thread, previous) {
					changes[key] = ch
				}
			}
			plan.Changes, plan.Checks, plan.Unified = changes, nil, true
			if err := cur.moveLegacyPieces(ctx, &plan, legacy); err != nil {
				return result, err
			}
		}
		return cur.commitPlan(ctx, plan)
	}
	if !errors.Is(e, fs.ErrNotExist) {
		return result, e
	}
	s := batchStore{dav: d, passphrase: c.Passphrase, batchID: id, remoteDir: remoteDir}
	raw, err := readBatchFile(s.filename("backups", id))
	if err != nil {
		return result, err
	}
	var manifest batchManifest
	if err := s.open(raw, id, &manifest); err != nil {
		return result, err
	}
	if !manifest.valid(2) {
		return result, errors.New("invalid session backup manifest")
	}
	objects, err := manifest.objects()
	if err != nil {
		return result, err
	}
	result.Conversations = len(manifest.Entries)
	back, stats, err := s.publish(ctx, objects, manifest)
	result.Backup, result.Remote = back, err == nil
	result.Uploaded, result.Skipped = stats.uploaded, stats.skipped
	result.Failed = append(result.Failed, stats.failed...)
	if len(stats.failed) > 0 {
		result.Conversations = stats.conversations
	}
	return result, err
}

func (manifest batchManifest) objects() ([]batchObject, error) {
	var objects []batchObject
	seen := make(map[string]bool)
	for _, key := range manifest.Entries {
		if !validBatchID(key) {
			return nil, errors.New("invalid session backup object ID")
		}
		parts, split := manifest.Parts[key]
		if !split {
			objects = append(objects, batchObject{"conversations", key})
			continue
		}
		if len(parts) < 2 {
			return nil, errors.New("invalid session backup parts")
		}
		for _, part := range parts {
			if !validBatchID(part) {
				return nil, errors.New("invalid session backup part ID")
			}
			if !seen[part] {
				objects = append(objects, batchObject{"parts", part})
				seen[part] = true
			}
		}
	}
	return objects, nil
}

func (s batchStore) load(ctx context.Context, id string) ([]sessions.BatchConversation, error) {
	return s.loadEntries(ctx, id, "")
}

func (s batchStore) loadEntries(ctx context.Context, id, entry string) ([]sessions.BatchConversation, error) {
	s.batchID = id
	var manifest batchManifest
	err := loadBatchObject(ctx, &s, "backups", id, &manifest)
	version := 2
	if errors.Is(err, errSessionBackupMissing) {
		s.batchID, version = "", 1
		err = loadBatchObject(ctx, &s, "backups", id, &manifest)
	}
	if err != nil {
		return nil, err
	}
	if !manifest.valid(version) {
		return nil, errors.New("invalid session backup manifest")
	}
	out := make([]sessions.BatchConversation, 0, len(manifest.Entries))
	for _, key := range manifest.Entries {
		if entry != "" && entry != key {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c, err := s.readEntry(ctx, manifest, key)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if entry != "" && len(out) == 0 {
		return nil, errors.New("conversation is missing from this backup")
	}
	return out, nil
}

func (s batchStore) readEntry(ctx context.Context, manifest batchManifest, key string) (sessions.BatchConversation, error) {
	var c sessions.BatchConversation
	if parts, split := manifest.Parts[key]; split {
		if !validBatchID(key) || len(parts) < 2 {
			return c, errors.New("invalid session backup parts")
		}
		reader := &batchPartReader{ctx: ctx, store: s, ids: parts}
		if err := openBatchStream(reader, key, &c); err != nil {
			return c, err
		}
	} else if err := loadBatchObject(ctx, &s, "conversations", key, &c); err != nil {
		return c, err
	}
	return c, nil
}

func batchDigest(raw []byte) string {
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func validBatchID(id string) bool {
	if len(id) != 2*sha256.Size || id != strings.ToLower(id) {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

func (s batchStore) objectPath(kind, id string) []string {
	if s.batchID == "" {
		return []string{legacyBatchDir, kind, id + ".sealed"}
	}
	if kind == "backups" {
		return []string{batchDir, s.batchID, "manifest.sealed"}
	}
	if kind == "parts" {
		return []string{batchDir, s.batchID, id + ".part.sealed"}
	}
	return []string{batchDir, s.batchID, id + ".sealed"}
}

func (s batchStore) filename(kind, id string) string {
	root := s.localRoot
	if root == "" {
		root = settings.Dir()
	}
	return filepath.Join(append([]string{root}, s.objectPath(kind, id)...)...)
}

func (s batchStore) url(kind, id string) string {
	return s.dav.url(append([]string{folder}, s.remoteObjectPath(kind, id)...)...)
}

func (s batchStore) remoteObjectPath(kind, id string) []string {
	parts := s.objectPath(kind, id)
	if s.current {
		parts[1] = currentSessionDir
	}
	if s.batchID != "" && s.remoteDir != batchDir {
		parts[0] = sessionDAVDir
		parts[2] = "session_" + parts[2]
	}
	if s.threadKey != "" {
		return []string{parts[0], parts[1], "threads", s.threadKey, parts[2]}
	}
	return parts
}

func (s batchStore) inThread(key string) batchStore {
	s.threadKey = key
	return s
}

func (s batchStore) keepLocal(kind, id string, plain []byte) ([]byte, error) {
	p := s.filename(kind, id)
	if raw, err := readBatchFile(p); err == nil {
		if err = s.open(raw, id, nil); err == nil || errors.Is(err, backup.ErrPassphrase) {
			return raw, err
		}
		// The source still matches this content ID. Regenerate a corrupt cache.
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	var compressed batchGzipBuffer
	z := gzip.NewWriter(&compressed)
	_, err := z.Write(plain)
	if err := errors.Join(err, z.Close()); err != nil {
		return nil, err
	}
	raw, err := sealBatchObject(compressed.Bytes(), s.passphrase)
	if err != nil {
		return nil, err
	}
	return raw, writeBatchFile(p, raw)
}

func sealBatchObject(data []byte, passphrase string) ([]byte, error) {
	raw, err := backup.SealData(data, passphrase)
	if err != nil {
		return nil, err
	}
	if len(raw) > batchPackedLimit {
		return nil, errBatchPackedTooLarge
	}
	return raw, nil
}

func writeBatchFile(p string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	return steady.WriteFile(p, raw, 0o600)
}

func readBatchFile(p string) ([]byte, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readBatchLimited(f)
}

func readBatchLimited(r io.Reader) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(r, batchPackedLimit+1))
	if err == nil && len(raw) > batchPackedLimit {
		err = errBatchPackedTooLarge
	}
	return raw, err
}

// open verifies the uncompressed checksum without buffering the entire JSON
// for verification alone. Callers that need the object can decode it at once.
func (s batchStore) open(raw []byte, id string, into any) error {
	zipped, err := backup.OpenData(raw, s.passphrase)
	if err != nil {
		return err
	}
	return openBatchStream(bytes.NewReader(zipped), id, into)
}

func openBatchStream(compressed io.Reader, id string, into any) error {
	z, err := gzip.NewReader(compressed)
	if err != nil {
		return err
	}
	defer z.Close()
	hash := sha256.New()
	if into == nil {
		if _, err := io.Copy(hash, z); err != nil {
			return err
		}
	} else {
		body := io.TeeReader(z, hash)
		dec := json.NewDecoder(body)
		if err := dec.Decode(into); err != nil {
			return err
		}
		if err := checkBatchJSONTail(io.MultiReader(dec.Buffered(), body)); err != nil {
			return err
		}
	}
	if hex.EncodeToString(hash.Sum(nil)) != id {
		return errors.New("session backup object did not match its checksum")
	}
	return nil
}

func checkBatchJSONTail(r io.Reader) error {
	var buf [32 << 10]byte
	for {
		n, err := r.Read(buf[:])
		if len(bytes.Trim(buf[:n], " \t\r\n")) != 0 {
			return errors.New("extra data after the session backup object")
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func loadBatchObject[T any](ctx context.Context, s *batchStore, kind, id string, into *T) error {
	value, err := readVerifiedBatchFile(ctx, s, kind, id, func(raw []byte) (T, error) {
		var decoded T
		err := s.open(raw, id, &decoded)
		return decoded, err
	})
	if err == nil {
		*into = value
	}
	return err
}

// A damaged local cache can be repaired by a verified remote copy. An
// unauthenticated replacement never reaches disk or the import target.
func readVerifiedBatchFile[T any](ctx context.Context, s *batchStore, kind, id string, decode func([]byte) (T, error)) (T, error) {
	var zero T
	if !validBatchID(id) {
		return zero, errors.New("enter a valid session backup ID")
	}
	if s.archive != nil {
		f, err := s.archive.Open(strings.Join(s.objectPath(kind, id), "/"))
		if err != nil {
			return zero, err
		}
		defer f.Close()
		raw, err := readBatchLimited(f)
		if err != nil {
			return zero, err
		}
		return decode(raw)
	}
	file := s.filename(kind, id)
	raw, localErr := readBatchFile(file)
	if localErr == nil {
		value, err := decode(raw)
		if err == nil {
			return value, nil
		}
		localErr = err
	}
	if s.dav == nil || ctx.Err() != nil {
		return zero, localErr
	}
	raw, err := s.remote(ctx, kind, id)
	if err != nil {
		if errors.Is(localErr, fs.ErrNotExist) {
			return zero, err
		}
		return zero, errors.Join(localErr, err)
	}
	value, err := decode(raw)
	if err != nil {
		return zero, err
	}
	if err := writeBatchFile(file, raw); err != nil {
		return zero, err
	}
	return value, nil
}

func (s *batchStore) remote(ctx context.Context, kind, id string) ([]byte, error) {
	var raw []byte
	err := retrySessionOperation(ctx, func() error {
		var err error
		raw, err = s.remoteAt(ctx, kind, id)
		if s.batchID != "" && s.remoteDir == "" && errors.Is(err, errSessionBackupMissing) {
			s.remoteDir = batchDir
			raw, err = s.remoteAt(ctx, kind, id)
			if err != nil {
				s.remoteDir = ""
			}
		}
		return err
	})
	return raw, err
}

func (s batchStore) remoteAt(ctx context.Context, kind, id string) ([]byte, error) {
	res, err := s.dav.do(ctx, http.MethodGet, s.url(kind, id), nil, nil)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusNotFound, http.StatusGone, http.StatusConflict:
		return nil, errSessionBackupMissing
	case http.StatusOK:
	default:
		return nil, fmt.Errorf("reading session backup from WebDAV: HTTP %d", res.StatusCode)
	}
	return readBatchLimited(res.Body)
}

func (s batchStore) publishRaw(ctx context.Context, kind, id string, raw []byte, exists bool) error {
	if err := s.verifyObject(raw, kind, id); err != nil {
		return err
	}
	targetURL := s.url(kind, id)
	headers := map[string]string{"If-None-Match": "*"}
	if exists {
		res, err := s.dav.send(ctx, http.MethodGet, targetURL, nil, map[string]string{"Accept-Encoding": "identity"})
		if err != nil {
			return err
		}
		prior, readErr := readBatchLimited(res.Body)
		res.Body.Close()
		if readErr != nil {
			return readErr
		}
		if res.StatusCode == http.StatusOK {
			if err := s.verifyResponse(kind, id, prior, res.Header.Get("ETag")); err == nil {
				return errNotModified
			} else if errors.Is(err, backup.ErrPassphrase) {
				return err
			}
			tag := res.Header.Get("ETag")
			if len(tag) < 2 || tag[0] != '"' || tag[len(tag)-1] != '"' {
				return errors.New("WebDAV needs a strong ETag to repair a damaged session object")
			}
			headers = map[string]string{"If-Match": tag}
		} else if res.StatusCode != http.StatusNotFound && res.StatusCode != http.StatusGone {
			return fmt.Errorf("reading session object: HTTP %d", res.StatusCode)
		}
	}
	res, err := s.dav.do(ctx, http.MethodPut, targetURL, raw, headers)
	if err != nil {
		return err
	}
	res.Body.Close()
	switch res.StatusCode {
	case http.StatusCreated, http.StatusNoContent, http.StatusOK:
		return s.verifyRemote(ctx, kind, id)
	case http.StatusPreconditionFailed:
		if err := s.verifyRemote(ctx, kind, id); err != nil {
			return err
		}
		return errNotModified
	case http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		return &sessionObjectRejected{res.Status}
	default:
		return fmt.Errorf("writing session backup to WebDAV: HTTP %d", res.StatusCode)
	}
}
