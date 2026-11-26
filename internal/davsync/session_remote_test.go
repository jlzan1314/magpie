package davsync

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/sessions"
)

type sizeAwareDAV struct {
	*fakeDAV
	nutstoreZeroHead atomic.Bool
	heads            atomic.Int64
	propfinds        atomic.Int64
	deletes          atomic.Int64
}

func newSizeAwareDAV() *sizeAwareDAV {
	return &sizeAwareDAV{
		fakeDAV: &fakeDAV{
			files: map[string][]byte{},
			etags: map[string]string{},
			dirs:  map[string]bool{"/dav": true},
		},
	}
}

func (s *sizeAwareDAV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		s.heads.Add(1)
		s.mu.Lock()
		b, ok := s.files[r.URL.Path]
		etag := s.etags[r.URL.Path]
		s.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if etag != "" {
			w.Header().Set("ETag", etag)
		}
		if s.nutstoreZeroHead.Load() {
			w.Header().Set("Content-Length", "0")
		} else {
			w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		}
		w.WriteHeader(http.StatusOK)
		return
	case "PROPFIND":
		s.propfinds.Add(1)
		depth := r.Header.Get("Depth")
		parent := strings.TrimSuffix(r.URL.Path, "/")
		s.mu.Lock()
		defer s.mu.Unlock()
		if depth == "0" {
			b, ok := s.files[r.URL.Path]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusMultiStatus)
			fmt.Fprint(w, `<multistatus xmlns="DAV:"><response><href>`)
			_ = xml.EscapeText(w, []byte(r.URL.Path))
			fmt.Fprintf(w, `</href><propstat><prop><getcontentlength>%d</getcontentlength></prop></propstat></response></multistatus>`, len(b))
			return
		}
		if !s.dirs[parent] {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusMultiStatus)
		fmt.Fprint(w, `<multistatus xmlns="DAV:">`)
		for d := range s.dirs {
			if pathpkg.Dir(d) == parent {
				fmt.Fprint(w, `<response><href>`)
				_ = xml.EscapeText(w, []byte(d+"/"))
				fmt.Fprint(w, `</href><propstat><prop><resourcetype><collection/></resourcetype></prop></propstat></response>`)
			}
		}
		for name, data := range s.files {
			if pathpkg.Dir(name) == parent {
				fmt.Fprint(w, `<response><href>`)
				_ = xml.EscapeText(w, []byte(name))
				fmt.Fprintf(w, `</href><propstat><prop><getcontentlength>%d</getcontentlength></prop></propstat></response>`, len(data))
			}
		}
		fmt.Fprint(w, `</multistatus>`)
		return
	case "COPY":
		target, err := url.Parse(r.Header.Get("Destination"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		data, ok := s.files[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if _, ok := s.files[target.Path]; ok {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		s.files[target.Path] = append([]byte(nil), data...)
		s.etags[target.Path] = s.etags[r.URL.Path]
		w.WriteHeader(http.StatusCreated)
		return
	case http.MethodDelete:
		s.deletes.Add(1)
		parent := strings.TrimSuffix(r.URL.Path, "/")
		s.mu.Lock()
		for file := range s.files {
			if file == parent || strings.HasPrefix(file, parent+"/") {
				delete(s.files, file)
			}
		}
		for d := range s.dirs {
			if d == parent || strings.HasPrefix(d, parent+"/") {
				delete(s.dirs, d)
			}
		}
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	default:
		s.fakeDAV.ServeHTTP(w, r)
	}
}

func TestCodexSessionSkipsMatchingRemoteSize(t *testing.T) {
	const id = "0196ce3b-81fc-7000-8000-7d8089eb9799"
	const pass = "size-skip-passphrase"
	name := filepath.Join("2026", "11", "25", "rollout-2026-11-25T18-00-00-"+id+".jsonl")
	meta := `{"type":"session_meta","payload":{"id":"` + id + `"}}` + "\n"
	turn1 := `{"type":"event_msg","payload":{"type":"user_message","message":"` + strings.Repeat("alpha ", 200) + `"}}` + "\n"
	turn2 := `{"type":"event_msg","payload":{"type":"user_message","message":"` + strings.Repeat("beta ", 200) + `"}}` + "\n"

	ctx := context.Background()
	davSrv := newSizeAwareDAV()
	srv := httptest.NewServer(davSrv)
	defer srv.Close()
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
	if err := Configure(Config{URL: srv.URL + "/dav", User: "me", Password: "pw", Passphrase: pass}); err != nil {
		t.Fatal(err)
	}
	rolloutPath := filepath.Join(home, "codex", "sessions", name)
	if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rolloutPath, []byte(meta+turn1), 0o600); err != nil {
		t.Fatal(err)
	}

	res, err := TransferCodexSession(ctx, "push", id)
	if err != nil || res.Changed != 1 || res.Skipped != 0 {
		t.Fatalf("initial push = %+v, %v", res, err)
	}
	davSrv.mu.Lock()
	if davSrv.puts != 1 || davSrv.gets != 2 {
		t.Fatalf("expected 1 PUT and 2 GETs on initial push, got puts=%d gets=%d", davSrv.puts, davSrv.gets)
	}
	davSrv.mu.Unlock()

	res, err = TransferCodexSession(ctx, "push", id)
	if err != nil || res.Changed != 0 || res.Skipped != 1 {
		t.Fatalf("repeat push = %+v, %v", res, err)
	}
	davSrv.mu.Lock()
	if davSrv.puts != 1 || davSrv.gets != 2 {
		t.Fatalf("repeat push should add no PUT/GET, got puts=%d gets=%d", davSrv.puts, davSrv.gets)
	}
	davSrv.mu.Unlock()

	res, err = TransferCodexSession(ctx, "pull", id)
	if err != nil || res.Changed != 0 || res.Skipped != 1 {
		t.Fatalf("pull matching size = %+v, %v", res, err)
	}
	davSrv.mu.Lock()
	if davSrv.gets != 2 {
		t.Fatalf("pull should add no GET, got gets=%d", davSrv.gets)
	}
	davSrv.mu.Unlock()

	davSrv.nutstoreZeroHead.Store(true)
	beforePropfinds := davSrv.propfinds.Load()
	res, err = TransferCodexSession(ctx, "push", id)
	if err != nil || res.Changed != 0 || res.Skipped != 1 {
		t.Fatalf("nutstore zero-head push = %+v, %v", res, err)
	}
	if davSrv.propfinds.Load() <= beforePropfinds {
		t.Fatal("expected PROPFIND fallback when HEAD returned Content-Length: 0")
	}
	davSrv.mu.Lock()
	if davSrv.puts != 1 || davSrv.gets != 2 {
		t.Fatalf("nutstore zero-head push should add no PUT/GET, got puts=%d gets=%d", davSrv.puts, davSrv.gets)
	}
	davSrv.mu.Unlock()
	davSrv.nutstoreZeroHead.Store(false)

	original, err := sessions.ReadCodexSnapshot(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	packed, err := sealCodexSession(original, pass, id)
	if err != nil {
		t.Fatal(err)
	}
	var fork []byte
	for letter := 'b'; letter <= 'z'; letter++ {
		candidate := []byte(meta + strings.ReplaceAll(turn1, "alpha", string(letter)+"lpha"))
		encoded, err := sealCodexSession([]sessions.CodexRollout{{Path: original[0].Path, Data: candidate}}, pass, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) == len(packed) {
			fork = candidate
			break
		}
	}
	if fork == nil {
		t.Fatal("fixture needs two divergent transcripts with equal packed lengths")
	}
	if err := os.WriteFile(rolloutPath, fork, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"push", "pull"} {
		if _, err := TransferCodexSession(ctx, action, id); !errors.Is(err, sessions.ErrCodexSyncConflict) {
			t.Fatalf("equal-sized fork %s: %v", action, err)
		}
	}

	if err := os.WriteFile(rolloutPath, []byte(meta+turn1+turn2), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err = TransferCodexSession(ctx, "push", id)
	if err != nil || res.Changed != 1 || res.Skipped != 0 {
		t.Fatalf("updated push = %+v, %v", res, err)
	}
	davSrv.mu.Lock()
	if davSrv.puts != 2 || davSrv.gets != 6 {
		t.Fatalf("updated push expected puts=2 gets=6 (including fork checks and read-back), got puts=%d gets=%d", davSrv.puts, davSrv.gets)
	}
	davSrv.mu.Unlock()
}

func TestSessionBatchSkipAndLocalChangeSync(t *testing.T) {
	const codexID = "0196ce3b-81fc-7000-8000-7d8089eb9801"
	const claudeID = "0196ce3b-81fc-7000-8000-7d8089eb9802"
	const stamp = "2026-11-25T10:00:00Z"
	const pass = "batch-size-skip-passphrase"

	ctx := context.Background()
	davSrv := newSizeAwareDAV()
	srv := httptest.NewServer(davSrv)
	defer srv.Close()

	home, project := t.TempDir(), t.TempDir()
	sessions.Reset()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, "claude"))
	if err := Configure(Config{
		URL:        srv.URL + "/dav",
		User:       "me",
		Password:   "pw",
		Passphrase: pass,
		Sessions:   true,
	}); err != nil {
		t.Fatal(err)
	}
	if text, err := SetupText(); err != nil {
		t.Fatalf("SetupText: %v", err)
	} else if got, err := OpenSetupText(text, pass); err != nil || !got.Sessions {
		t.Fatalf("OpenSetupText Sessions=%v, err=%v", got.Sessions, err)
	}

	codexNative := filepath.Join(home, "codex", "sessions", "2026", "11", "25", "rollout-2026-11-25T10-00-00-"+codexID+".jsonl")
	claudeNative := filepath.Join(home, "claude", "projects", "example", claudeID+".jsonl")
	writeCodex := func(msg, reply string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(codexNative), 0o700); err != nil {
			t.Fatal(err)
		}
		lines := fmt.Sprintf(`{"timestamp":%q,"type":"session_meta","payload":{"id":%q,"cwd":%q,"timestamp":%q}}
{"timestamp":%q,"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":%q}]}}
{"timestamp":%q,"type":"event_msg","payload":{"type":"user_message","message":%q}}
`, stamp, codexID, project, stamp, stamp, msg, stamp, msg)
		if reply != "" {
			lines += fmt.Sprintf(`{"timestamp":%q,"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":%q}]}}
`, stamp, reply)
		}
		if err := os.WriteFile(codexNative, []byte(lines), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeClaude := func(msg string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(claudeNative), 0o700); err != nil {
			t.Fatal(err)
		}
		line, err := json.Marshal(map[string]any{
			"type": "user", "timestamp": stamp, "cwd": project, "sessionId": claudeID,
			"uuid": "0196ce3b-81fc-7000-8000-7d8089eb9803", "message": map[string]string{"role": "user", "content": msg},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(claudeNative, append(line, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	writeCodex("Codex conversation one", "")
	writeClaude("Claude conversation two")

	first, err := RunSessionBatch(ctx, SessionBatchRequest{All: true})
	if err != nil || first.Conversations != 2 || first.Uploaded != 3 || first.Skipped != 0 {
		t.Fatalf("first batch = %+v, %v", first, err)
	}
	davSrv.mu.Lock()
	if davSrv.puts != 3 || davSrv.gets != 3 {
		t.Fatalf("first batch expected puts=3 gets=3, got puts=%d gets=%d", davSrv.puts, davSrv.gets)
	}
	davSrv.mu.Unlock()

	repeat, err := RunSessionBatch(ctx, SessionBatchRequest{All: true})
	if err != nil || repeat.Backup != first.Backup || repeat.Uploaded != 0 || repeat.Skipped != 3 {
		t.Fatalf("repeat batch = %+v, %v", repeat, err)
	}
	davSrv.mu.Lock()
	if davSrv.puts != 3 || davSrv.gets != 3 {
		t.Fatalf("repeat batch should skip without PUT/GET, got puts=%d gets=%d", davSrv.puts, davSrv.gets)
	}
	davSrv.mu.Unlock()

	writeCodex("Codex conversation one", "New assistant reply after local change.")
	future := time.Now().Add(2 * time.Second)
	_ = os.Chtimes(codexNative, future, future)
	sessions.Reset()
	second, err := RunSessionBatch(ctx, SessionBatchRequest{All: true})
	if err != nil || second.Backup == first.Backup || second.Conversations != 2 || second.Uploaded != 2 || second.Skipped != 1 {
		t.Fatalf("incremental batch = %+v, %v", second, err)
	}
	davSrv.mu.Lock()
	if davSrv.puts != 5 || davSrv.gets != 5 {
		t.Fatalf("incremental batch expected puts=5 gets=5, got puts=%d gets=%d", davSrv.puts, davSrv.gets)
	}
	davSrv.mu.Unlock()

	store := batchStore{passphrase: pass, batchID: second.Backup}
	manifestData, err := readBatchFile(store.filename("backups", second.Backup))
	if err != nil {
		t.Fatal(err)
	}
	var current batchManifest
	if err := store.open(manifestData, second.Backup, &current); err != nil {
		t.Fatal(err)
	}
	truncatedPath := "/dav/magpie/" + batchDir + "/" + second.Backup + "/" + current.Entries[0] + ".sealed"
	davSrv.mu.Lock()
	orig, exists := davSrv.files[truncatedPath]
	if !exists || len(orig) <= 10 {
		davSrv.mu.Unlock()
		t.Fatalf("expected deduplicated object file at %s", truncatedPath)
	}
	davSrv.files[truncatedPath] = orig[:10]
	davSrv.mu.Unlock()

	repaired, err := RunSessionBatch(ctx, SessionBatchRequest{All: true})
	if err != nil || repaired.Uploaded != 1 || repaired.Skipped != 2 {
		t.Fatalf("truncated repair = %+v, %v", repaired, err)
	}

	if err := DeleteSessionBackup(ctx, second.Backup, "webdav"); err != nil {
		t.Fatal(err)
	}
	davSrv.mu.Lock()
	for name := range davSrv.files {
		if strings.Contains(name, "/"+second.Backup+"/") {
			t.Fatalf("deleted backup still has %s", name)
		}
	}
	davSrv.mu.Unlock()

	synced1, err := SyncLocalSessions(ctx)
	if err != nil || synced1.Backup != second.Backup || synced1.Uploaded != 3 || synced1.Skipped != 0 {
		t.Fatalf("SyncLocalSessions first = %+v, %v", synced1, err)
	}
	davSrv.mu.Lock()
	putsBeforeNoop := davSrv.puts
	getsBeforeNoop := davSrv.gets
	davSrv.mu.Unlock()

	synced2, err := SyncLocalSessions(ctx)
	if err != nil || synced2.Backup != synced1.Backup || synced2.Uploaded != 0 || synced2.Skipped != 3 {
		t.Fatalf("SyncLocalSessions noop = %+v, %v", synced2, err)
	}
	davSrv.mu.Lock()
	if davSrv.puts != putsBeforeNoop || davSrv.gets != getsBeforeNoop {
		t.Fatalf("SyncLocalSessions noop should issue 0 PUT/GET, got delta puts=%d gets=%d", davSrv.puts-putsBeforeNoop, davSrv.gets-getsBeforeNoop)
	}
	davSrv.mu.Unlock()

	writeCodex("Codex conversation one", "Another local edit synced automatically.")
	future2 := future.Add(2 * time.Second)
	_ = os.Chtimes(codexNative, future2, future2)
	sessions.Reset()
	if err := SyncNow(ctx); err != nil {
		t.Fatalf("SyncNow with Sessions=true: %v", err)
	}
	v := Status()
	if !v.Sessions || v.SessionsError != "" || v.SessionsBackup == "" || v.SessionsBackup == synced1.Backup {
		t.Fatalf("unexpected Sessions status after SyncNow: %+v", v)
	}
}

func TestSessionBatchParts(t *testing.T) {
	const pass = "session part test passphrase"
	ctx := context.Background()
	fake := newSizeAwareDAV()
	server := httptest.NewServer(fake)
	defer server.Close()
	c := Config{URL: server.URL + "/dav", User: "me", Password: "pw", Passphrase: pass}
	d, err := newDAV(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	s := batchStore{dav: d, passphrase: pass}
	// Use a small part size to exercise splitting without a large test upload.
	content := make([]byte, 2048)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range content {
		content[i] = byte(rng.Uint32())
	}
	entry := sessions.BatchConversation{
		Sources: []sessions.BatchSource{{Agent: "codex", ID: "sample"}},
		Parts:   []sessions.Part{{Role: "user", Kind: "text", Text: base64.StdEncoding.EncodeToString(content)}},
	}
	id, sent, err := s.save(ctx, []sessions.BatchConversation{entry}, true, 512)
	if err != nil || sent.uploaded < 3 {
		t.Fatalf("split backup upload: %+v, %v", sent, err)
	}
	saved, unchanged, err := s.save(ctx, []sessions.BatchConversation{entry}, true, 512)
	if err != nil || saved != id || unchanged.uploaded != 0 || unchanged.skipped != sent.uploaded {
		t.Fatalf("split backup repeat: %s, %+v, %v", saved, unchanged, err)
	}
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	read, err := s.load(ctx, id)
	if err != nil || len(read) != 1 || read[0].Fingerprint() != entry.Fingerprint() {
		t.Fatalf("split backup read: %d conversations, %v", len(read), err)
	}

	s.batchID = id
	var manifest batchManifest
	if err := s.loadObject(ctx, "backups", id, &manifest); err != nil {
		t.Fatal(err)
	}
	parts := manifest.Parts[manifest.Entries[0]]
	if manifest.Version != 3 || len(parts) < 2 {
		t.Fatalf("split backup manifest: %+v", manifest)
	}
	first := "/" + strings.Join(append([]string{"dav", folder}, s.objectPath("parts", parts[0])...), "/")
	second := "/" + strings.Join(append([]string{"dav", folder}, s.objectPath("parts", parts[1])...), "/")
	fake.mu.Lock()
	fake.files[first] = fake.files[second]
	fake.mu.Unlock()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if _, err := s.load(ctx, id); err == nil || !strings.Contains(err.Error(), "part did not match its checksum") {
		t.Fatalf("backup with a replaced part read without its checksum error: %v", err)
	}
}

func TestSessionBatchVerifiesLargeGzipStream(t *testing.T) {
	// An authenticated and valid JSON object that expands beyond the old
	// 128 MiB limit. The padding exercises the streaming verifier.
	const pass = "large portable object test"
	var packed bytes.Buffer
	z := gzip.NewWriter(&packed)
	h := sha256.New()
	out := io.MultiWriter(z, h)
	if _, err := io.WriteString(out, `{"version":2,"entries":["sample"]}`); err != nil {
		t.Fatal(err)
	}
	block := bytes.Repeat([]byte(" "), 1<<20)
	for range 129 {
		if _, err := out.Write(block); err != nil {
			t.Fatal(err)
		}
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sealBatchObject(packed.Bytes(), pass)
	if err != nil {
		t.Fatal(err)
	}
	var got batchManifest
	s := batchStore{passphrase: pass}
	if err := s.open(raw, hex.EncodeToString(h.Sum(nil)), &got); err != nil || got.Version != 2 || len(got.Entries) != 1 {
		t.Fatalf("large gzip object: %+v, %v", got, err)
	}
}

func TestSessionSyncStatusRetiresOldSizeErrors(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	c := Config{URL: "https://dav.example.invalid", User: "me", Password: "pw", Passphrase: "session status test", Sessions: true}
	if err := Configure(c); err != nil {
		t.Fatal(err)
	}
	config, ok := Load()
	if !ok {
		t.Fatal("missing WebDAV configuration")
	}
	writeError := func(message string) View {
		saveState(state{Key: stateKey(config), Sessions: &sessionSyncState{Error: message, Backup: "previous-backup"}})
		return Status()
	}
	for _, legacy := range []string{"session exceeds the 16 MiB portable transcript limit", "session exceeds the 64 MiB uncompressed transcript limit"} {
		v := writeError("reading codex:example: " + legacy)
		if v.SessionsError != "" || v.SessionsBackup != "previous-backup" {
			t.Fatalf("legacy diagnostic or backup reference changed incorrectly: %q, %q", v.SessionsError, v.SessionsBackup)
		}
	}
	const current = "writing session backup to WebDAV: HTTP 413"
	if v := writeError(current); v.SessionsError != current {
		t.Fatalf("current diagnostic: got %q, want %q", v.SessionsError, current)
	}
}
