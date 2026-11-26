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
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/yetone/magpie/internal/backup"
	"github.com/yetone/magpie/internal/sessions"
	"github.com/yetone/magpie/internal/settings"
	"github.com/yetone/magpie/internal/steady"
)

const batchDir = "session-batches-v2"
const legacyBatchDir = "session-batches-v1"
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
	Sources []sessions.BatchSource `json:"sources"`
	All     bool                   `json:"all"`
	Backups []string               `json:"backups"`
	Targets []string               `json:"targets"`
	Cwd     string                 `json:"cwd"`
}

type SessionBatchResult struct {
	Backup        string                 `json:"backup,omitempty"`
	Remote        bool                   `json:"remote"`
	Conversations int                    `json:"conversations"`
	Duplicates    int                    `json:"duplicates"`
	Uploaded      int                    `json:"uploaded"`
	Skipped       int                    `json:"skipped"`
	Imports       []sessions.BatchImport `json:"imports,omitempty"`
}

type batchManifest struct {
	Version int                 `json:"version"`
	Entries []string            `json:"entries"`
	Parts   map[string][]string `json:"parts,omitempty"`
}

type batchStore struct {
	dav        *dav
	passphrase string
	batchID    string
	onPrepared func(string)
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

func runSessionBatch(ctx context.Context, c Config, req SessionBatchRequest, onPrepared func(string)) (SessionBatchResult, error) {
	ctx = withSessionDAVTimeouts(ctx)
	var result SessionBatchResult
	d, err := newDAV(c)
	if err != nil {
		return result, err
	}
	store := batchStore{dav: d, passphrase: c.Passphrase, onPrepared: onPrepared}
	if err := sessions.CheckBatchTargets(req.Targets, strings.TrimSpace(req.Cwd)); err != nil {
		return result, err
	}
	build := req.All || len(req.Sources) > 0 || len(req.Backups) > 1
	entries, err := sessions.CollectBatch(ctx, req.Sources, req.All)
	if err != nil {
		return result, err
	}
	for _, name := range req.Backups {
		old, err := store.load(ctx, strings.TrimSpace(name))
		if err != nil {
			return result, err
		}
		entries = append(entries, old...)
	}
	if len(entries) == 0 {
		return result, errEmptySessionBatch
	}
	merged := sessions.MergeBatch(entries)
	result.Conversations = len(merged)
	result.Duplicates = len(entries) - len(merged)
	if build {
		var stats batchPublishStats
		result.Backup, stats, err = store.save(ctx, merged, !c.FollowServer, batchPartSize)
		result.Remote = err == nil && !c.FollowServer
		result.Uploaded, result.Skipped = stats.uploaded, stats.skipped
		if err != nil {
			// A local backup ID is still useful after a network failure.
			return result, err
		}
	} else {
		result.Backup = strings.TrimSpace(req.Backups[0])
	}
	if len(req.Targets) != 0 {
		result.Imports, err = sessions.ImportBatch(ctx, merged, req.Targets, strings.TrimSpace(req.Cwd))
	}
	return result, err
}

type batchPublishStats struct {
	uploaded int
	skipped  int
}

func (s batchStore) save(ctx context.Context, entries []sessions.BatchConversation, publish bool, partSize int) (string, batchPublishStats, error) {
	var stats batchPublishStats
	manifest := batchManifest{Version: 2}
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
		s.onPrepared(s.batchID)
	}
	return s.publish(ctx, objects)
}

// publish sends the prepared files and then the manifest. It is also the
// resume path, where the manifest on disk supplies the same objects.
func (s batchStore) publish(ctx context.Context, objects []batchObject) (string, batchPublishStats, error) {
	var stats batchPublishStats
	hint := filepath.Join(settings.Dir(), batchDir, "last-"+stateKey(Config{URL: s.dav.base.String(), User: s.dav.user, Passphrase: s.passphrase}))
	last, _ := os.ReadFile(hint)
	previous := s
	previous.batchID = string(last)
	sizes, err := s.dav.listRemoteSizes(ctx, folder, batchDir, s.batchID)
	if err != nil {
		return s.batchID, stats, err
	}
	var previousSizes map[string]int64
	if validBatchID(previous.batchID) && previous.batchID != s.batchID {
		previousSizes, err = s.dav.listRemoteSizes(ctx, folder, batchDir, previous.batchID)
		if err != nil {
			return s.batchID, stats, err
		}
	}
	ready := false
	for _, object := range append(objects, batchObject{"backups", s.batchID}) {
		kind, id := object.kind, object.id
		file := s.filename(kind, id)
		info, err := os.Stat(file)
		if err != nil {
			return s.batchID, stats, err
		}
		name := filepath.Base(file)
		same, exists, err := s.dav.remoteSizeMatches(ctx, s.url(kind, id), info.Size(), sizes, name)
		if err != nil {
			return s.batchID, stats, err
		}
		if same {
			stats.skipped++
			continue
		}
		if !ready {
			for _, dirs := range [][]string{{folder}, {folder, batchDir}, {folder, batchDir, s.batchID}} {
				if err := s.dav.ensureSessionCollection(ctx, dirs); err != nil {
					return s.batchID, stats, err
				}
			}
			ready = true
		}
		if size, found := previousSizes[name]; kind != "backups" && !exists && found && size == info.Size() {
			copied, err := s.copyObject(ctx, previous, object, size)
			if err != nil {
				return s.batchID, stats, err
			}
			if copied {
				stats.skipped++
				continue
			}
		}
		raw, err := readBatchFile(file)
		if err != nil {
			return s.batchID, stats, err
		}
		if err := s.publishRaw(ctx, kind, id, raw, exists); errors.Is(err, errNotModified) {
			stats.skipped++
			continue
		} else if err != nil {
			return s.batchID, stats, err
		}
		stats.uploaded++
	}
	if previous.batchID != s.batchID {
		_ = steady.WriteFile(hint, []byte(s.batchID), 0o600)
	}
	return s.batchID, stats, nil
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
		same, _, err := s.dav.remoteSizeMatches(ctx, target, size, nil, "")
		if err != nil || same {
			return same, err
		}
		raw, err := s.remote(ctx, object.kind, object.id)
		if err != nil {
			return false, err
		}
		err = s.verifyObject(raw, object.kind, object.id)
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
func resumeSessionBatch(ctx context.Context, c Config, id string) (SessionBatchResult, error) {
	ctx = withSessionDAVTimeouts(ctx)
	var result SessionBatchResult
	if !validBatchID(id) {
		return result, errors.New("invalid pending session backup ID")
	}
	d, err := newDAV(c)
	if err != nil {
		return result, err
	}
	s := batchStore{dav: d, passphrase: c.Passphrase, batchID: id}
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
	var objects []batchObject
	seen := make(map[string]bool)
	for _, key := range manifest.Entries {
		if !validBatchID(key) {
			return result, errors.New("invalid session backup object ID")
		}
		parts, split := manifest.Parts[key]
		if !split {
			objects = append(objects, batchObject{"conversations", key})
			continue
		}
		if len(parts) < 2 {
			return result, errors.New("invalid session backup parts")
		}
		for _, part := range parts {
			if !validBatchID(part) {
				return result, errors.New("invalid session backup part ID")
			}
			if !seen[part] {
				objects = append(objects, batchObject{"parts", part})
				seen[part] = true
			}
		}
	}
	result.Conversations = len(manifest.Entries)
	back, stats, err := s.publish(ctx, objects)
	result.Backup, result.Remote = back, err == nil
	result.Uploaded, result.Skipped = stats.uploaded, stats.skipped
	return result, err
}

func (s batchStore) load(ctx context.Context, id string) ([]sessions.BatchConversation, error) {
	s.batchID = id
	var manifest batchManifest
	err := s.loadObject(ctx, "backups", id, &manifest)
	version := 2
	if errors.Is(err, errSessionBackupMissing) {
		s.batchID, version = "", 1
		err = s.loadObject(ctx, "backups", id, &manifest)
	}
	if err != nil {
		return nil, err
	}
	if !manifest.valid(version) {
		return nil, errors.New("invalid session backup manifest")
	}
	out := make([]sessions.BatchConversation, 0, len(manifest.Entries))
	for _, key := range manifest.Entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var c sessions.BatchConversation
		if parts, split := manifest.Parts[key]; split {
			if !validBatchID(key) || len(parts) < 2 {
				return nil, errors.New("invalid session backup parts")
			}
			reader := &batchPartReader{ctx: ctx, store: s, ids: parts}
			if err := openBatchStream(reader, key, &c); err != nil {
				return nil, err
			}
		} else if err := s.loadObject(ctx, "conversations", key, &c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
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
	return filepath.Join(append([]string{settings.Dir()}, s.objectPath(kind, id)...)...)
}

func (s batchStore) url(kind, id string) string {
	return s.dav.url(append([]string{folder}, s.objectPath(kind, id)...)...)
}

func (s batchStore) keepLocal(kind, id string, plain []byte) ([]byte, error) {
	p := s.filename(kind, id)
	if raw, err := readBatchFile(p); err == nil {
		err = s.open(raw, id, nil)
		return raw, err
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

func (s batchStore) loadObject(ctx context.Context, kind, id string, into any) error {
	raw, fetched, err := s.loadRaw(ctx, kind, id)
	if err != nil {
		return err
	}
	if err := s.open(raw, id, into); err != nil {
		return err
	}
	if fetched {
		return writeBatchFile(s.filename(kind, id), raw)
	}
	return nil
}

func (s batchStore) loadRaw(ctx context.Context, kind, id string) ([]byte, bool, error) {
	if !validBatchID(id) {
		return nil, false, errors.New("enter a valid session backup ID")
	}
	p := s.filename(kind, id)
	raw, err := readBatchFile(p)
	fetched := errors.Is(err, fs.ErrNotExist)
	if fetched {
		raw, err = s.remote(ctx, kind, id)
	}
	return raw, fetched, err
}

func (s batchStore) remote(ctx context.Context, kind, id string) ([]byte, error) {
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
			if err := s.verifyObject(prior, kind, id); err == nil {
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
		got, err := s.remote(ctx, kind, id)
		if err != nil {
			return err
		}
		return s.verifyObject(got, kind, id)
	case http.StatusPreconditionFailed:
		got, err := s.remote(ctx, kind, id)
		if err != nil {
			return err
		}
		if err := s.verifyObject(got, kind, id); err != nil {
			return err
		}
		return errNotModified
	default:
		return fmt.Errorf("writing session backup to WebDAV: HTTP %d", res.StatusCode)
	}
}
