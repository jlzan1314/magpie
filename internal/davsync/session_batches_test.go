package davsync

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/yetone/magpie/internal/sessions"
)

func TestSessionBatch(t *testing.T) {
	const codexID = "0196ce3b-81fc-7000-8000-7d8089eb9715"
	const claudeID = "0196ce3b-81fc-7000-8000-7d8089eb9716"
	const stamp = "2026-11-25T10:00:00Z"
	const message = "Please inspect the example project before changing it."
	ctx := context.Background()
	fake := &fakeDAV{files: map[string][]byte{}, etags: map[string]string{}, dirs: map[string]bool{"/dav": true}}
	var publishFailure atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if publishFailure.Load() && r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "session_manifest.sealed") {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.Method == "PROPFIND" || r.Method == http.MethodDelete {
			fake.mu.Lock()
			defer fake.mu.Unlock()
			parent := strings.TrimSuffix(r.URL.Path, "/")
			if r.Method == "PROPFIND" {
				if !fake.dirs[parent] {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.WriteHeader(http.StatusMultiStatus)
				fmt.Fprint(w, `<multistatus xmlns="DAV:">`)
				write := func(p string) {
					fmt.Fprint(w, "<response><href>")
					_ = xml.EscapeText(w, []byte(p))
					fmt.Fprint(w, "</href></response>")
				}
				for d := range fake.dirs {
					if pathpkg.Dir(d) == parent {
						write(d + "/")
					}
				}
				for name := range fake.files {
					if pathpkg.Dir(name) == parent {
						write(name)
					}
				}
				fmt.Fprint(w, "</multistatus>")
				return
			}
			for file := range fake.files {
				if file == parent || strings.HasPrefix(file, parent+"/") {
					delete(fake.files, file)
				}
			}
			for d := range fake.dirs {
				if d == parent || strings.HasPrefix(d, parent+"/") {
					delete(fake.dirs, d)
				}
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		fake.ServeHTTP(w, r)
	}))
	defer server.Close()
	c := Config{URL: server.URL + "/dav", User: "me", Password: "pw", Passphrase: "test session batch passphrase"}
	from, to, project := t.TempDir(), t.TempDir(), t.TempDir()
	useHome := func(home string) {
		t.Helper()
		sessions.Reset()
		t.Setenv("HOME", home)
		t.Setenv("USERPROFILE", home)
		t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
		t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
		t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, "claude"))
		if err := Configure(c); err != nil {
			t.Fatal(err)
		}
	}
	writeLines := func(path string, lines ...map[string]any) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		for _, line := range lines {
			if err := enc.Encode(line); err != nil {
				t.Fatal(err)
			}
		}
	}
	codexNative := filepath.Join(from, "codex", "sessions", "2026", "11", "25", "rollout-2026-11-25T10-00-00-"+codexID+".jsonl")
	claudeNative := filepath.Join(from, "claude", "projects", "example", claudeID+".jsonl")
	writeCodex := func(replies ...string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(codexNative), 0o700); err != nil {
			t.Fatal(err)
		}
		lines := fmt.Sprintf(`{"timestamp":%q,"type":"session_meta","payload":{"id":%q,"cwd":%q,"timestamp":%q}}
{"timestamp":%q,"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":%q}]}}
{"timestamp":%q,"type":"event_msg","payload":{"type":"user_message","message":%q}}
`, stamp, codexID, project, stamp, stamp, message, stamp, message)
		for _, reply := range replies {
			if reply == "" {
				continue
			}
			lines += fmt.Sprintf(`{"timestamp":%q,"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":%q}]}}
`, stamp, reply)
		}
		if err := os.WriteFile(codexNative, []byte(lines), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeCodex("")
	writeLines(claudeNative, map[string]any{
		"type": "user", "timestamp": stamp, "cwd": project, "sessionId": claudeID,
		"uuid": "0196ce3b-81fc-7000-8000-7d8089eb9717", "message": map[string]string{"role": "user", "content": message},
	})

	useHome(from)
	picked := []sessions.BatchSource{{Agent: "codex", ID: codexID}, {Agent: "claude", ID: claudeID}}
	first, err := RunSessionBatch(ctx, SessionBatchRequest{Sources: picked})
	if err != nil || first.Conversations != 1 || first.Duplicates != 1 || !first.Remote {
		t.Fatalf("dedupe: %+v, %v, %#v", first, err, sessions.BatchCandidates())
	}
	fake.mu.Lock()
	if len(fake.files) != 2 {
		t.Errorf("expected an object and a manifest, got %d files", len(fake.files))
	}
	for p, raw := range fake.files {
		if !strings.HasPrefix(p, "/dav/magpie/sessions/"+currentSessionDir+"/") || !strings.HasPrefix(pathpkg.Base(p), "session_") {
			t.Errorf("new backup used an unexpected WebDAV path: %s", p)
		}
		if bytes.Contains(raw, []byte(message)) {
			t.Error("WebDAV contains plaintext conversation")
		}
	}
	fake.mu.Unlock()
	existing, err := RunSessionBatch(ctx, SessionBatchRequest{Backups: []string{first.Backup}, Targets: []string{"claude", "codex"}})
	if err != nil || len(existing.Imports) != 2 {
		t.Fatalf("existing native sessions: %+v, %v", existing, err)
	}
	for i, id := range []string{claudeID, codexID} {
		if got := existing.Imports[i]; got.Session != id || !got.Existing {
			t.Fatalf("an identical native session was duplicated: %+v", got)
		}
	}

	writeCodex("First result.")
	sessions.Reset()
	second, err := RunSessionBatch(ctx, SessionBatchRequest{Sources: picked[:1], Backups: []string{first.Backup}})
	if err != nil || second.Conversations != 1 || second.Backup != first.Backup {
		t.Fatalf("append merge: %+v, %v", second, err)
	}
	writeCodex("Different result.")
	sessions.Reset()
	publishFailure.Store(true)
	blocked, err := RunSessionBatch(ctx, SessionBatchRequest{Sources: picked[:1], Backups: []string{second.Backup}, Targets: []string{"codex", "claude"}})
	if err == nil || len(blocked.Imports) != 0 || blocked.Remote {
		t.Fatalf("backup failed before distribution: %+v %v", blocked, err)
	}
	publishFailure.Store(false)
	third, err := RunSessionBatch(ctx, SessionBatchRequest{Sources: picked[:1], Backups: []string{second.Backup}})
	if err != nil || third.Backup != first.Backup || third.Conversations != 1 {
		t.Fatalf("replacement and backup retry: %+v, %v", third, err)
	}
	useHome(to)
	if err := SetFollowServer(true); err != nil {
		t.Fatal(err)
	}
	restore := SessionBatchRequest{Backups: []string{third.Backup}, Targets: []string{"codex", "claude", "codex"}}
	targets, err := RunSessionBatch(ctx, restore)
	if err != nil || len(targets.Imports) != 2 {
		t.Fatalf("distribution: %+v, %v", targets, err)
	}
	for _, item := range targets.Imports {
		if item.Error != "" || item.Existing || item.Session == "" || !strings.Contains(item.Command, item.Session) {
			t.Fatalf("invalid native import: %+v", item)
		}
	}
	sessions.Reset()
	for _, item := range targets.Imports {
		s, ok := sessions.Find(item.Agent, item.Session)
		if !ok {
			t.Fatalf("%s can't find %s", item.Agent, item.Session)
		}
		if s.Cwd != filepath.Join(to, "sessions") {
			t.Fatalf("%s imported into %q", item.Agent, s.Cwd)
		}
		if transcript, err := sessions.TranscriptOf(s); err != nil || len(transcript.Parts) == 0 {
			t.Fatalf("%s can't read the imported transcript: %v", item.Agent, err)
		}
	}
	again, err := RunSessionBatch(ctx, restore)
	if err != nil || len(again.Imports) != 2 {
		t.Fatalf("repeat: %+v, %v", again, err)
	}
	for i, item := range again.Imports {
		if !item.Existing || item.Session != targets.Imports[i].Session {
			t.Fatalf("repeat created a duplicate: %+v", item)
		}
	}
	check := func(id string, local, remote bool) {
		t.Helper()
		list, err := ListSessionBackups(ctx)
		if err != nil || list.WebDAVError != "" {
			t.Fatalf("backup list %+v, %v", list, err)
		}
		for _, b := range list.Items {
			if b.ID == id {
				if b.Local != local || b.WebDAV != remote {
					t.Fatalf("backup scope %+v", b)
				}
				return
			}
		}
		t.Fatalf("missing backup %s", id)
	}
	check(third.Backup, true, true)
	if err := DeleteSessionBackup(ctx, third.Backup, "webdav"); err == nil {
		t.Fatal("follower deleted server backup")
	}
	if err := DeleteSessionBackup(ctx, third.Backup, "local"); err != nil {
		t.Fatal(err)
	}
	check(third.Backup, false, true)
	for _, item := range targets.Imports {
		if _, ok := sessions.Find(item.Agent, item.Session); !ok {
			t.Fatalf("deleting a backup removed a native session: %+v", item)
		}
	}
	useHome(from)
	if err := DeleteSessionBackup(ctx, third.Backup, "webdav"); err != nil {
		t.Fatal(err)
	}
	check(third.Backup, true, false)
	if first.Backup != third.Backup {
		t.Fatal("updates created separate DAV backups")
	}
	fake.mu.Lock()
	for p := range fake.files {
		if strings.Contains(p, "/"+currentSessionDir+"/") {
			t.Errorf("deleted WebDAV backup still contains %s", p)
		}
	}
	fake.mu.Unlock()

	// The transcript exceeds the old 64 MiB limit, but the encrypted gzip
	// object fits and another home can read it.
	large := strings.Repeat("0123456789abcdef", (18<<20)/16)
	writeCodex(large, "second "+large, "third "+large, "fourth "+large)
	useHome(from)
	big, err := RunSessionBatch(ctx, SessionBatchRequest{Sources: picked[:1]})
	if err != nil || !big.Remote || big.Conversations != 1 {
		t.Fatalf("long session upload: %+v, %v", big, err)
	}
	prefix := "/dav/" + folder + "/" + sessionDAVDir + "/" + currentSessionDir + "/"
	fake.mu.Lock()
	for p, raw := range fake.files {
		if strings.HasPrefix(p, prefix) && len(raw) >= 16<<20 {
			t.Errorf("compressible session object %s is still %d bytes", pathpkg.Base(p), len(raw))
		}
	}
	fake.mu.Unlock()
	useHome(t.TempDir())
	read, err := RunSessionBatch(ctx, SessionBatchRequest{Backups: []string{big.Backup}})
	if err != nil || read.Conversations != 1 {
		t.Fatalf("long session download: %+v, %v", read, err)
	}
}
