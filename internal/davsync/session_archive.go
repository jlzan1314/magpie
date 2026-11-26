package davsync

import (
	"archive/zip"
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
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/yetone/magpie/internal/backup"
	"github.com/yetone/magpie/internal/sessions"
)

func init() { backup.CaptureSessions = writeSessionArchive }

type localSessionArchive struct {
	Library sessionLibrary
	Files   map[string]string // portable object path -> SHA-256 of the sealed file
}
type archivePool struct {
	dir   string
	files map[string]string
}

func (p archivePool) Open(name string) (fs.File, error) {
	id := p.files[name]
	if !validBatchID(id) {
		return nil, fs.ErrNotExist
	}
	return os.Open(filepath.Join(p.dir, id+".sealed"))
}

func writeSessionArchive(ctx context.Context, w io.Writer, pass, folder string) (string, error) {
	store := batchStore{passphrase: pass, batchID: currentSessionID, localRoot: path("session-archive-" + batchDigest([]byte(folder+"\x00"+pass)))}
	cache := filepath.Join(store.localRoot, "state.sealed")
	previous := localSessionArchive{}
	if sealed, e := os.ReadFile(cache); e == nil {
		if raw, e := backup.OpenData(sealed, pass); e == nil {
			_ = json.Unmarshal(raw, &previous)
		}
	}
	if previous.Library.Format != sessionLibraryFormat {
		previous = localSessionArchive{Library: newSessionLibrary()}
	}
	pool := filepath.Join(folder, backup.SessionPool)
	if err := os.MkdirAll(pool, 0700); err != nil {
		return "", err
	}
	objectPath := func(o batchObject) string { return strings.Join(store.objectPath(o.kind, o.id), "/") }
	marksFile := filepath.Join(store.localRoot, "pool-marks.json")
	fileMarks := map[string]string{}
	if b, e := os.ReadFile(marksFile); e == nil {
		_ = json.Unmarshal(b, &fileMarks)
	}
	if fileMarks == nil {
		fileMarks = map[string]string{}
	}
	checked := map[string]bool{}
	validObject := func(id string) bool {
		if !validBatchID(id) {
			return false
		}
		if ok, exists := checked[id]; exists {
			return ok
		}
		name := filepath.Join(pool, id+".sealed")
		mark := sessions.SettledFileMark(name)
		ok := mark != "" && fileMarks[id] == mark
		if !ok {
			if fi, e := os.Lstat(name); e == nil && fi.Mode().IsRegular() {
				if f, e := os.Open(name); e == nil {
					hash := sha256.New()
					_, e = io.Copy(hash, f)
					f.Close()
					ok = e == nil && hex.EncodeToString(hash.Sum(nil)) == id
				}
			}
		}
		if ok {
			fileMarks[id] = mark
		}
		checked[id] = ok
		return ok
	}
	for k, t := range previous.Library.Threads {
		for _, p := range t.Pieces {
			for _, o := range p.objects() {
				cipher := previous.Files[objectPath(o)]
				if !validObject(cipher) {
					delete(previous.Library.Threads, k)
				}
			}
		}
	}
	plan, err := store.prepareCurrent(ctx, previous.Library, nil, true, false, nil)
	if err != nil {
		return "", err
	}
	if len(plan.Failed) > 0 {
		return "", errors.New(plan.Failed[0].Error)
	}
	next := localSessionArchive{Library: newSessionLibrary(), Files: map[string]string{}}
	marks := store.marks()
	for k := range plan.Checks {
		next.Library.Threads[k] = previous.Library.Threads[k]
	}
	for k, ch := range plan.Changes {
		next.Library.Threads[k] = ch.Thread
		marks[k] = currentMark{ch.Mark, ch.Thread.Revision}
	}
	if len(next.Library.Threads) == 0 {
		return "", nil
	}
	for _, t := range next.Library.Threads {
		for _, p := range t.Pieces {
			for _, o := range p.objects() {
				if err = ctx.Err(); err != nil {
					return "", err
				}
				key := objectPath(o)
				if next.Files[key] != "" {
					continue
				}
				cipher := previous.Files[key]
				if !validObject(cipher) {
					data, e := readBatchFile(store.filename(o.kind, o.id))
					if e != nil {
						return "", e
					}
					if e = store.verifyObject(data, o.kind, o.id); e != nil {
						return "", e
					}
					cipher = batchDigest(data)
					if e = writeBatchFile(filepath.Join(pool, cipher+".sealed"), data); e != nil {
						return "", e
					}
				}
				next.Files[key] = cipher
				checked[cipher] = true
			}
		}
	}
	raw, err := json.Marshal(next)
	if err != nil {
		return "", err
	}
	id := batchDigest(raw)
	var packed batchGzipBuffer
	gz := gzip.NewWriter(&packed)
	_, err = gz.Write(raw)
	if err = errors.Join(err, gz.Close()); err != nil {
		return "", err
	}
	sealed, err := backup.SealData(packed.Bytes(), pass)
	if err != nil {
		return "", err
	}
	z := zip.NewWriter(w)
	f, err := z.CreateHeader(&zip.FileHeader{Name: batchDir + "/" + id + "/manifest.sealed", Method: zip.Store})
	if err != nil {
		return "", err
	}
	if _, err = f.Write(sealed); err != nil {
		return "", err
	}
	for _, cipher := range slices.Compact(slices.Sorted(maps.Values(next.Files))) {
		if _, err = z.CreateHeader(&zip.FileHeader{Name: backup.SessionPool + "/" + cipher + ".sealed", Method: zip.Store}); err != nil {
			return "", err
		}
	}
	if err = z.Close(); err != nil {
		return "", err
	}
	// Only encrypted metadata is cached; pool files belong to the retained
	// versions, whose manifest markers protect them during pruning.
	if metadata, e := backup.SealData(raw, pass); e == nil {
		_ = writeBatchFile(cache, metadata)
	}
	if b, e := json.Marshal(marks); e == nil {
		_ = writeBatchFile(store.checkpoint(), b)
	}
	if b, e := json.Marshal(fileMarks); e == nil {
		_ = writeBatchFile(marksFile, b)
	}
	// Newly prepared bodies are now in the destination pool.
	for _, ch := range plan.Changes {
		for _, p := range ch.Thread.Pieces {
			for _, o := range p.objects() {
				_ = os.Remove(store.filename(o.kind, o.id))
			}
		}
	}
	return id, nil
}

func RestoreAutoSessions(ctx context.Context, name, pass, cwd string, targets []string) ([]sessions.BatchImport, error) {
	archive, id, configured, err := backup.OpenAutoSessions(name)
	if err != nil {
		return nil, err
	}
	defer archive.Close()
	if pass == "" {
		pass = configured
	}
	store := batchStore{passphrase: pass, archive: archive, batchID: id}
	entries := []sessions.BatchConversation{}
	pooled := slices.ContainsFunc(archive.File, func(f *zip.File) bool { return strings.HasPrefix(f.Name, backup.SessionPool+"/") })
	if pooled {
		var snapshot localSessionArchive
		if err = loadBatchObject(ctx, &store, "backups", id, &snapshot); err != nil {
			return nil, err
		}
		if snapshot.Library.Format != sessionLibraryFormat {
			return nil, fmt.Errorf("unknown local session backup format %q", snapshot.Library.Format)
		}
		store.batchID = currentSessionID
		store.archive = archivePool{archive.Pool, snapshot.Files}
		for _, key := range snapshot.Library.visible() {
			c, e := store.readThread(ctx, snapshot.Library.Threads[key])
			if e != nil {
				return nil, e
			}
			entries = append(entries, c)
		}
	} else {
		entries, err = store.load(ctx, id)
		if err != nil {
			return nil, err
		}
	}
	return sessions.ImportBatch(ctx, entries, targets, cwd)
}
