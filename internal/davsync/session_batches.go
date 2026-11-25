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
	var result SessionBatchResult
	c, ok := Load()
	if !ok || c.S3() || c.GitHub() {
		return result, errors.New("set up WebDAV sync before backing up sessions")
	}
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
		return result, errors.New("select sessions or a session backup")
	}
	merged := sessions.MergeBatch(entries)
	result.Conversations = len(merged)
	result.Duplicates = len(entries) - len(merged)
	if build {
		result.Backup, err = store.save(ctx, merged, !c.FollowServer)
		result.Remote = err == nil && !c.FollowServer
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

func (s batchStore) save(ctx context.Context, entries []sessions.BatchConversation, publish bool) (string, error) {
	manifest := batchManifest{Version: 2, Entries: make([]string, 0, len(entries))}
	for _, entry := range entries {
		raw, err := json.Marshal(entry)
		if err != nil {
			return "", err
		}
		manifest.Entries = append(manifest.Entries, batchDigest(raw))
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	s.batchID = batchDigest(manifestData)
	for i, entry := range entries {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		raw, err := json.Marshal(entry)
		if err != nil {
			return "", err
		}
		if _, err := s.keepLocal("conversations", manifest.Entries[i], raw); err != nil {
			return "", err
		}
	}
	if _, err := s.keepLocal("backups", s.batchID, manifestData); err != nil {
		return "", err
	}
	if !publish {
		return s.batchID, nil
	}
	for _, dirs := range [][]string{{folder}, {folder, batchDir}, {folder, batchDir, s.batchID}} {
		if err := s.dav.ensureSessionCollection(ctx, dirs); err != nil {
			return s.batchID, err
		}
	}
	for _, id := range manifest.Entries {
		if err := s.publish(ctx, "conversations", id); err != nil {
			return s.batchID, err
		}
	}
	return s.batchID, s.publish(ctx, "backups", s.batchID)
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

func (s batchStore) publish(ctx context.Context, kind, id string) error {
	p := s.filename(kind, id)
	raw, err := readBatchFile(p)
	if err != nil {
		return err
	}
	if _, err := s.open(raw, id); err != nil {
		return err
	}
	res, err := s.dav.do(ctx, http.MethodPut, s.url(kind, id), raw, map[string]string{"If-None-Match": "*"})
	if err != nil {
		return err
	}
	res.Body.Close()
	switch res.StatusCode {
	case http.StatusCreated, http.StatusNoContent, http.StatusOK, http.StatusPreconditionFailed:
	default:
		return fmt.Errorf("writing session backup to WebDAV: HTTP %d", res.StatusCode)
	}
	got, err := s.remote(ctx, kind, id)
	if err != nil {
		return err
	}
	_, err = s.open(got, id)
	return err
}
