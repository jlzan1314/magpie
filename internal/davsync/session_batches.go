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
const batchPlainLimit = 2 * sessions.MaxBatchConversation

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
	Version int      `json:"version"`
	Entries []string `json:"entries"`
}

type batchStore struct {
	dav        *dav
	passphrase string
	batchID    string
}

// RunSessionBatch captures selected local sessions and optional earlier
// batches, merges them, publishes the encrypted backup and then imports
// the result. Importing a single existing backup works on a DAV follower.
func RunSessionBatch(ctx context.Context, req SessionBatchRequest) (SessionBatchResult, error) {
	c, ok := Load()
	if !ok || c.S3() || c.GitHub() {
		return SessionBatchResult{}, errors.New("set up WebDAV sync before backing up sessions")
	}
	return runSessionBatch(ctx, c, req)
}

var errEmptySessionBatch = errors.New("select sessions or a session backup")

func runSessionBatch(ctx context.Context, c Config, req SessionBatchRequest) (SessionBatchResult, error) {
	var result SessionBatchResult
	d, err := newDAV(c)
	if err != nil {
		return result, err
	}
	store := batchStore{dav: d, passphrase: c.Passphrase}
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
		result.Backup, stats, err = store.save(ctx, merged, !c.FollowServer)
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

func (s batchStore) save(ctx context.Context, entries []sessions.BatchConversation, publish bool) (string, batchPublishStats, error) {
	var stats batchPublishStats
	manifest := batchManifest{Version: 2}
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return "", stats, err
		}
		manifest.Entries = append(manifest.Entries, batchDigest(data))
	}
	plain, err := json.Marshal(manifest)
	if err != nil {
		return "", stats, err
	}
	s.batchID = batchDigest(plain)
	for i, entry := range entries {
		if err := ctx.Err(); err != nil {
			return "", stats, err
		}
		data, err := json.Marshal(entry)
		if err != nil {
			return "", stats, err
		}
		if _, err := s.keepLocal("conversations", manifest.Entries[i], data); err != nil {
			return "", stats, err
		}
	}
	if _, err := s.keepLocal("backups", s.batchID, plain); err != nil {
		return "", stats, err
	}
	if !publish {
		return s.batchID, stats, nil
	}
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
	for i := 0; i <= len(manifest.Entries); i++ {
		kind, id := "backups", s.batchID
		if i < len(manifest.Entries) {
			kind, id = "conversations", manifest.Entries[i]
		}
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
		if size, found := previousSizes[name]; kind == "conversations" && !exists && found && size == info.Size() {
			copied, err := s.copyConversation(ctx, previous, id, size)
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

func (s batchStore) copyConversation(ctx context.Context, previous batchStore, id string, size int64) (bool, error) {
	target := s.url("conversations", id)
	res, err := s.dav.send(ctx, "COPY", previous.url("conversations", id), nil, map[string]string{"Destination": target, "Overwrite": "F"})
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
		raw, err := s.remote(ctx, "conversations", id)
		if err != nil {
			return false, err
		}
		_, err = s.open(raw, id)
		return err == nil, err
	case http.StatusNotFound, http.StatusGone, http.StatusConflict, http.StatusForbidden, http.StatusBadRequest, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return false, nil
	default:
		return false, fmt.Errorf("copying WebDAV conversation: HTTP %d", res.StatusCode)
	}
}

var errSessionBackupMissing = errors.New("session backup was not found")

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
	if manifest.Version != version || len(manifest.Entries) == 0 {
		return nil, errors.New("invalid session backup manifest")
	}
	out := make([]sessions.BatchConversation, 0, len(manifest.Entries))
	for _, key := range manifest.Entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var c sessions.BatchConversation
		if err := s.loadObject(ctx, "conversations", key, &c); err != nil {
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
		_, err = s.open(raw, id)
		return raw, err
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if len(plain) > batchPlainLimit {
		return nil, errors.New("session backup object is too large")
	}
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	_, err := z.Write(plain)
	if err := errors.Join(err, z.Close()); err != nil {
		return nil, err
	}
	raw, err := backup.SealData(compressed.Bytes(), s.passphrase)
	if err != nil {
		return nil, err
	}
	return raw, writeBatchFile(p, raw)
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
	// Base64 and the encrypted envelope cost more bytes than gzip itself.
	limit := int64(2 * batchPlainLimit)
	raw, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err == nil && int64(len(raw)) > limit {
		err = errors.New("session backup object is too large")
	}
	return raw, err
}

func (s batchStore) open(raw []byte, id string) ([]byte, error) {
	zipped, err := backup.OpenData(raw, s.passphrase)
	if err != nil {
		return nil, err
	}
	z, err := gzip.NewReader(bytes.NewReader(zipped))
	if err != nil {
		return nil, err
	}
	defer z.Close()
	plain, err := io.ReadAll(io.LimitReader(z, batchPlainLimit+1))
	if err != nil {
		return nil, err
	}
	if len(plain) > batchPlainLimit || batchDigest(plain) != id {
		return nil, errors.New("session backup object did not match its checksum")
	}
	return plain, nil
}

func (s batchStore) loadObject(ctx context.Context, kind, id string, into any) error {
	if !validBatchID(id) {
		return errors.New("enter a valid session backup ID")
	}
	p := s.filename(kind, id)
	raw, err := readBatchFile(p)
	fetched := errors.Is(err, fs.ErrNotExist)
	if fetched {
		raw, err = s.remote(ctx, kind, id)
	}
	if err != nil {
		return err
	}
	plain, err := s.open(raw, id)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(plain, into); err != nil {
		return err
	}
	if fetched {
		return writeBatchFile(p, raw)
	}
	return nil
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
	if _, err := s.open(raw, id); err != nil {
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
			if _, err := s.open(prior, id); err == nil {
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
		_, err = s.open(got, id)
		return err
	case http.StatusPreconditionFailed:
		got, err := s.remote(ctx, kind, id)
		if err != nil {
			return err
		}
		if _, err := s.open(got, id); err != nil {
			return err
		}
		return errNotModified
	default:
		return fmt.Errorf("writing session backup to WebDAV: HTTP %d", res.StatusCode)
	}
}
