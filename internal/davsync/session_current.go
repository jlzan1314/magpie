package davsync

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/yetone/magpie/internal/backup"
	"github.com/yetone/magpie/internal/sessions"
)

// The public handle is stable. The encrypted manifest at this collection is
// the sole commit point. Payloads are immutable and verified before it moves.
const currentSessionDir = "current"

var currentSessionID = batchDigest([]byte("magpie shared WebDAV session backup v1"))
var sharedSessionHead = batchDigest([]byte("magpie unified WebDAV session catalog"))

type currentPiece struct {
	ID         string   `json:"id"`
	Parts      []string `json:"parts,omitempty"`
	Collection string   `json:"collection,omitempty"`
}
type currentThread struct {
	Meta        sessions.BatchConversation `json:"meta"`
	Origin      string                     `json:"origin"`
	Device      string                     `json:"device"`
	Revision    string                     `json:"revision"`
	Fingerprint string                     `json:"fingerprint"`
	Count       int                        `json:"count"`
	Pieces      []currentPiece             `json:"pieces"`
}

func (t currentThread) identity() string {
	key := batchDigest([]byte(t.Meta.SyncIdentity()))
	if !t.Meta.HasSyncName() {
		key = batchDigest([]byte(batchDigest([]byte(t.Origin)) + key))
	}
	return key
}

type sessionLibrary struct {
	Format  string                   `json:"format"`
	Threads map[string]currentThread `json:"threads"`
}

const sessionLibraryFormat = "magpie-current-sessions-1"

func newSessionLibrary() sessionLibrary {
	return sessionLibrary{Format: sessionLibraryFormat, Threads: map[string]currentThread{}}
}

func currentStore(c Config, d *dav) batchStore {
	return batchStore{dav: d, passphrase: c.Passphrase, batchID: currentSessionID, remoteDir: sessionDAVDir, localRoot: path("session-current-" + stateKey(c)), current: true, headKey: sharedSessionHead}
}
func (s batchStore) headURL() string {
	return s.dav.url(folder, sessionDAVDir, currentSessionDir, "heads", "session_"+s.headKey+"_session_manifest.sealed")
}
func (s batchStore) headCachePath() string {
	return filepath.Join(s.localRoot, "heads", s.headKey+".json")
}

type cachedSessionHead struct {
	ETag string
	Body []byte
}

func (s batchStore) cachedHead() cachedSessionHead {
	var cached cachedSessionHead
	if b, e := os.ReadFile(s.headCachePath()); e == nil {
		_ = json.Unmarshal(b, &cached)
	}
	return cached
}
func (s batchStore) readHead(ctx context.Context) (sessionLibrary, string, error) {
	head := newSessionLibrary()
	cache := s.headCachePath()
	savedHead := s.cachedHead()
	if strongSessionTag(savedHead.ETag) {
		res, e := s.dav.send(ctx, http.MethodHead, s.headURL(), nil, map[string]string{"Accept-Encoding": "identity"})
		if e != nil {
			return head, "", e
		}
		res.Body.Close()
		if res.StatusCode == http.StatusOK && res.Header.Get("ETag") == savedHead.ETag && (res.ContentLength <= 0 || res.ContentLength == int64(len(savedHead.Body))) {
			m, e := s.decodeHead(savedHead.Body)
			if e == nil {
				return m, savedHead.ETag, nil
			}
		}
		if res.StatusCode == http.StatusNotFound || res.StatusCode == http.StatusGone || res.StatusCode == http.StatusConflict {
			return head, "", nil
		}
	}
	res, err := s.dav.do(ctx, http.MethodGet, s.headURL(), nil, map[string]string{"Accept-Encoding": "identity"})
	if err != nil {
		return head, "", err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound || res.StatusCode == http.StatusGone || res.StatusCode == http.StatusConflict {
		return head, "", nil
	}
	if res.StatusCode != http.StatusOK {
		return head, "", fmt.Errorf("reading current session backup: HTTP %d", res.StatusCode)
	}
	raw, err := readBatchLimited(res.Body)
	if err != nil {
		return head, "", err
	}
	decoded, err := s.decodeHead(raw)
	tag := res.Header.Get("ETag")
	if err == nil {
		savedHead.ETag, savedHead.Body = tag, raw
		if b, e := json.Marshal(savedHead); e == nil {
			_ = writeBatchFile(cache, b)
		}
	}
	if err == nil && tag == "" {
		tag = "unversioned"
	}
	return decoded, tag, err
}

func (s batchStore) readLibrary(ctx context.Context, remote bool) (sessionLibrary, error) {
	if remote {
		if head, tag, err := s.readHead(ctx); err != nil || tag != "" {
			return head, err
		}
	} else if cached := s.cachedHead(); len(cached.Body) > 0 {
		return s.decodeHead(cached.Body)
	}
	return s.readLegacyLibrary(ctx, remote)
}

func (s batchStore) readLegacyLibrary(ctx context.Context, remote bool) (sessionLibrary, error) {
	library := newSessionLibrary()
	var keys []string
	if remote {
		names, err := s.dav.listRemoteSizes(ctx, folder, sessionDAVDir, currentSessionDir, "heads")
		if err != nil {
			return library, err
		}
		if names == nil {
			return library, errors.New("WebDAV could not list session manifests")
		}
		for name := range names {
			k := strings.TrimSuffix(strings.TrimPrefix(name, "session_"), "_session_manifest.sealed")
			if validBatchID(k) && name == "session_"+k+"_session_manifest.sealed" {
				keys = append(keys, k)
			}
		}
	} else {
		files, err := os.ReadDir(filepath.Join(s.localRoot, "heads"))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return library, err
		}
		for _, f := range files {
			k := strings.TrimSuffix(f.Name(), ".json")
			if validBatchID(k) && f.Name() == k+".json" {
				keys = append(keys, k)
			}
		}
	}
	slices.Sort(keys)
	var mu sync.Mutex
	var failures []error
	workers := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for _, key := range keys {
		if key == sharedSessionHead {
			continue
		}
		if ctx.Err() != nil {
			break
		}
		workers <- struct{}{}
		wg.Go(func() {
			defer func() { <-workers }()
			shard := s
			shard.headKey = key
			var h sessionLibrary
			var err error
			if remote {
				h, _, err = shard.readHead(ctx)
			} else {
				h, err = shard.decodeHead(shard.cachedHead().Body)
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failures = append(failures, err)
				return
			}
			for _, t := range h.Threads {
				identity := t.identity()
				if old := library.Threads[identity]; preferCurrentThread(t, old) {
					library.Threads[identity] = t
				}
			}
		})
	}
	wg.Wait()
	return library, errors.Join(append(failures, ctx.Err())...)
}
func strongSessionTag(tag string) bool {
	return len(tag) >= 2 && tag[0] == '"' && tag[len(tag)-1] == '"'
}
func (s batchStore) decodeHead(raw []byte) (sessionLibrary, error) {
	zipped, err := backup.OpenData(raw, s.passphrase)
	if err != nil {
		return sessionLibrary{}, err
	}
	z, err := gzip.NewReader(bytes.NewReader(zipped))
	if err != nil {
		return sessionLibrary{}, err
	}
	defer z.Close()
	dec := json.NewDecoder(z)
	var saved sessionLibrary
	if err = dec.Decode(&saved); err != nil {
		return sessionLibrary{}, err
	}
	if saved.Format != sessionLibraryFormat || saved.Threads == nil {
		return sessionLibrary{}, errors.New("invalid current session manifest")
	}
	for key, t := range saved.Threads {
		if !validBatchID(key) || !validBatchID(t.Revision) || !validBatchID(t.Fingerprint) || t.Count <= 0 || len(t.Pieces) == 0 || len(t.Meta.Sources) == 0 {
			return sessionLibrary{}, errors.New("invalid current session manifest entry")
		}
		for _, p := range t.Pieces {
			if !validBatchID(p.ID) || (p.Collection != "" && !validBatchID(p.Collection)) {
				return sessionLibrary{}, errors.New("invalid session piece")
			}
			for _, id := range p.Parts {
				if !validBatchID(id) {
					return sessionLibrary{}, errors.New("invalid session part")
				}
			}
		}
	}
	return saved, nil
}

func (s batchStore) writeHead(ctx context.Context, head sessionLibrary, tag string) error {
	raw, err := json.Marshal(head)
	if err != nil {
		return err
	}
	var packed batchGzipBuffer
	z := gzip.NewWriter(&packed)
	_, err = z.Write(raw)
	if err = errors.Join(err, z.Close()); err != nil {
		return err
	}
	encrypted, err := sealBatchObject(packed.Bytes(), s.passphrase)
	if err != nil {
		return err
	}
	cond := map[string]string{"If-None-Match": "*"}
	if tag != "" {
		if len(tag) < 2 || tag[0] != '"' || tag[len(tag)-1] != '"' {
			return errors.New("WebDAV needs a strong ETag to update the shared session backup")
		}
		cond = map[string]string{"If-Match": tag}
	}
	res, err := s.dav.do(ctx, http.MethodPut, s.headURL(), encrypted, cond)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode == http.StatusPreconditionFailed || res.StatusCode == http.StatusConflict {
		return errChanged
	}
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("updating current session manifest: HTTP %d", res.StatusCode)
	}
	return nil
}

func currentThreadKey(ref sessions.BatchSource) string {
	raw, _ := json.Marshal(ref)
	return batchDigest(raw)
}
func (p currentPiece) objects() []batchObject {
	if len(p.Parts) == 0 {
		return []batchObject{{"conversations", p.ID}}
	}
	out := make([]batchObject, 0, len(p.Parts))
	for _, id := range p.Parts {
		out = append(out, batchObject{"parts", id})
	}
	return out
}
func (s batchStore) readThread(ctx context.Context, t currentThread) (sessions.BatchConversation, error) {
	c := t.Meta
	c.Parts = nil
	for _, p := range t.Pieces {
		if err := ctx.Err(); err != nil {
			return c, err
		}
		m := batchManifest{}
		if len(p.Parts) > 0 {
			m.Parts = map[string][]string{p.ID: p.Parts}
		}
		piece, err := s.inThread(p.Collection).readEntry(ctx, m, p.ID)
		if err != nil {
			return c, err
		}
		c.Parts = append(c.Parts, piece.Parts...)
	}
	raw, err := json.Marshal(c)
	if err == nil && (len(c.Parts) != t.Count || batchDigest(raw) != t.Revision) {
		err = errors.New("restored session does not match its committed revision")
	}
	return c, err
}

func (h sessionLibrary) visible() []string {
	return slices.Sorted(maps.Keys(h.Threads))
}
func (s batchStore) loadCurrent(ctx context.Context, entry string) ([]sessions.BatchConversation, error) {
	head, err := s.readLibrary(ctx, true)
	if err != nil || len(head.Threads) == 0 {
		if errors.Is(err, backup.ErrPassphrase) || ctx.Err() != nil {
			return nil, err
		}
		cached, e := s.readLibrary(ctx, false)
		if e != nil {
			if err != nil {
				return nil, err
			}
			return nil, errSessionBackupMissing
		}
		head = cached
		s.dav = nil // restore a verified local copy offline
	}
	var out []sessions.BatchConversation
	for _, key := range slices.Sorted(maps.Keys(head.Threads)) {
		if entry != "" && entry != key {
			continue
		}
		c, err := s.readThread(ctx, head.Threads[key])
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, errSessionBackupMissing
	}
	return out, nil
}

func (s batchStore) currentChoice(ctx context.Context, row RestoreChoice, verify bool) RestoreChoice {
	var h sessionLibrary
	var err error
	if row.WebDAV {
		h, err = s.readLibrary(ctx, true)
	} else {
		h, err = s.readLibrary(ctx, false)
	}
	if err == nil && len(h.Threads) > 0 {
		manifests := map[string]batchManifest{}
		for _, key := range h.visible() {
			t := h.Threads[key]
			if row.Device == "" {
				row.Device = t.Device
			}
			row.Entries = append(row.Entries, restoreEntry{key, t.Meta.Title})
			for _, p := range t.Pieces {
				m := manifests[p.Collection]
				m.Entries = append(m.Entries, p.ID)
				if len(p.Parts) > 0 {
					if m.Parts == nil {
						m.Parts = map[string][]string{}
					}
					m.Parts[p.ID] = p.Parts
				}
				manifests[p.Collection] = m
			}
			if !verify && len(row.Entries) == 2 {
				break
			}
		}
		if verify {
			for collection, m := range manifests {
				if err = s.inThread(collection).checkManifestFiles(ctx, m, row.WebDAV); err != nil {
					break
				}
			}
		}
		row.Ready, row.RemoteReady = verify && err == nil, row.WebDAV && verify && err == nil
	}
	if err != nil {
		row.Problem = err.Error()
	}
	return row
}
