package davsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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
		if publishFailure.Load() && r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/backups/") {
			w.WriteHeader(http.StatusServiceUnavailable)
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
	writeCodex := func(reply string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(codexNative), 0o700); err != nil {
			t.Fatal(err)
		}
		lines := fmt.Sprintf(`{"timestamp":%q,"type":"session_meta","payload":{"id":%q,"cwd":%q,"timestamp":%q}}
{"timestamp":%q,"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":%q}]}}
{"timestamp":%q,"type":"event_msg","payload":{"type":"user_message","message":%q}}
`, stamp, codexID, project, stamp, stamp, message, stamp, message)
		if reply != "" {
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
	for _, raw := range fake.files {
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
	if err != nil || second.Conversations != 1 || second.Duplicates != 1 || second.Backup == first.Backup {
		t.Fatalf("append merge: %+v, %v", second, err)
	}
	writeCodex("Different result.")
	sessions.Reset()
	publishFailure.Store(true)
	blocked, err := RunSessionBatch(ctx, SessionBatchRequest{Sources: picked[:1], Backups: []string{second.Backup}, Targets: []string{"codex", "claude"}})
	if err == nil || blocked.Backup == "" || len(blocked.Imports) != 0 || blocked.Conversations != 2 {
		t.Fatalf("backup failed before distribution: %+v %v", blocked, err)
	}
	publishFailure.Store(false)
	third, err := RunSessionBatch(ctx, SessionBatchRequest{Sources: picked[:1], Backups: []string{second.Backup}})
	if err != nil || third.Backup != blocked.Backup || third.Conversations != 2 {
		t.Fatalf("branch preservation and backup retry: %+v, %v", third, err)
	}
	useHome(to)
	if err := SetFollowServer(true); err != nil {
		t.Fatal(err)
	}
	restore := SessionBatchRequest{Backups: []string{third.Backup}, Targets: []string{"codex", "claude", "codex"}, Cwd: project}
	targets, err := RunSessionBatch(ctx, restore)
	if err != nil || len(targets.Imports) != 4 {
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
		if transcript, err := sessions.TranscriptOf(s); err != nil || len(transcript.Parts) == 0 {
			t.Fatalf("%s can't read the imported transcript: %v", item.Agent, err)
		}
	}
	again, err := RunSessionBatch(ctx, restore)
	if err != nil || len(again.Imports) != 4 {
		t.Fatalf("repeat: %+v, %v", again, err)
	}
	for i, item := range again.Imports {
		if !item.Existing || item.Session != targets.Imports[i].Session {
			t.Fatalf("repeat created a duplicate: %+v", item)
		}
	}
}
