package davsync

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/yetone/magpie/internal/backup"
	"github.com/yetone/magpie/internal/sessions"
)

func TestWebDAVCodexSessionTransfer(t *testing.T) {
	const id = "0196ce3b-81fc-7000-8000-7d8089eb9715"
	const pass = "fixture-only passphrase"
	name := filepath.Join("2026", "11", "25", "rollout-2026-11-25T18-00-00-"+id+".jsonl")
	meta := `{"type":"session_meta","payload":{"id":"` + id + `"}}` + "\n"
	line := `{"type":"event_msg","payload":{"type":"user_message","message":"` + strings.Repeat("test conversation ", 1000) + `"}}` + "\n"
	first, appended := meta+line, meta+line+line
	ctx := context.Background()
	source, dest := t.TempDir(), t.TempDir()
	fake := &fakeDAV{files: map[string][]byte{}, etags: map[string]string{}, dirs: map[string]bool{"/dav": true}}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	c := Config{URL: srv.URL + "/dav", User: "me", Password: "pw", Passphrase: pass}
	if err := Configure(c); err != nil {
		t.Fatal(err)
	}
	write := func(root, data string, compressed bool) {
		t.Helper()
		p := filepath.Join(root, "sessions", name)
		b := []byte(data)
		if compressed {
			p += ".zst"
			z, err := zstd.NewWriter(nil)
			if err != nil {
				t.Fatal(err)
			}
			b = z.EncodeAll(b, nil)
			z.Close()
		}
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(home, op string, changed int) {
		t.Helper()
		t.Setenv("CODEX_HOME", home)
		got, err := TransferCodexSession(ctx, op, id)
		if err != nil || got.Changed != changed {
			t.Fatalf("%s = %+v, %v", op, got, err)
		}
	}
	read := func(root, want string) {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, "sessions", name))
		if err != nil || string(data) != want {
			t.Fatalf("restored rollout differs: %v", err)
		}
	}

	write(source, first, true)
	run(source, "push", 1)
	run(source, "push", 0)
	fake.mu.Lock()
	sealed := bytes.Clone(fake.files["/dav/magpie/sessions-v1/codex/"+id+".magpie-session"])
	independent := len(fake.files) == 1
	fake.mu.Unlock()
	if !independent || len(sealed) == 0 || len(sealed) >= len(first)/2 || bytes.Contains(sealed, []byte("test conversation")) {
		t.Fatal("session was not stored separately, compressed and sealed")
	}
	if _, err := openCodexSession(sealed, "wrong", id); !errors.Is(err, backup.ErrPassphrase) {
		t.Fatalf("wrong passphrase accepted: %v", err)
	}
	run(dest, "pull", 1)
	read(dest, first)
	write(source, appended, true)
	run(source, "push", 1)
	run(dest, "pull", 1)
	run(dest, "pull", 0)
	read(dest, appended)

	divergent := first + `{"type":"event_msg","payload":{"message":"different turn"}}` + "\n"
	write(dest, divergent, false)
	for _, op := range []string{"push", "pull"} {
		if _, err := TransferCodexSession(ctx, op, id); !errors.Is(err, sessions.ErrCodexSyncConflict) {
			t.Fatalf("%s overwrote a divergent history: %v", op, err)
		}
	}
	read(dest, divergent)
	readArgs, err := openCodexSession(sealed, pass, id)
	if err != nil {
		t.Fatal(err)
	}
	readArgs[0].Path = "../config.toml"
	if _, err := sessions.ImportCodexSnapshot(ctx, id, readArgs); err == nil {
		t.Fatal("archive could write outside the Codex sessions tree")
	}
	if err := SetFollowServer(true); err != nil {
		t.Fatal(err)
	}
	if _, err := TransferCodexSession(ctx, "push", id); err == nil {
		t.Fatal("the WebDAV follower could upload a session")
	}
}
