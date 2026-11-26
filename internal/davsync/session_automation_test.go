package davsync

import (
	"archive/zip"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/backup"
	"github.com/yetone/magpie/internal/sessions"
)

func TestAutomaticBackupsCarryEncryptedSessionsOffline(t *testing.T) {
	temp := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(temp, "config"))
	t.Setenv("CODEX_HOME", filepath.Join(temp, "codex"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(temp, "claude"))
	sessions.Reset()
	config := backup.AutoConfig{Sessions: true, Passphrase: "offline-only test", Hours: 1, Keep: 1, Dir: t.TempDir()}
	if err := backup.SaveAuto(config); err != nil {
		t.Fatal(err)
	}
	project := t.TempDir()
	backupTestThread(t, testThreadA, "sensitive offline fixture", project)
	at := time.Now()
	made, err := backup.AutoTick(at, "test")
	if err != nil || made == nil || !made.Sessions {
		t.Fatalf("scheduled session backup: %+v %v", made, err)
	}
	companion := func(name string) string {
		return filepath.Join(config.Dir, "session_"+strings.TrimSuffix(name, ".json")+".zip")
	}
	z, err := zip.OpenReader(companion(made.Name))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range z.File {
		if !strings.HasSuffix(f.Name, ".sealed") {
			t.Fatalf("unsealed member %s", f.Name)
		}
	}
	z.Close()
	if _, err := RestoreAutoSessions(context.Background(), made.Name, "wrong", project, []string{"claude"}); err == nil {
		t.Fatal("wrong passphrase accepted")
	}
	for i := 0; i < 2; i++ {
		out, err := RestoreAutoSessions(context.Background(), made.Name, "", project, []string{"claude"})
		if err != nil || len(out) != 1 || out[0].Error != "" || out[0].Existing != (i > 0) {
			t.Fatalf("restore %d: %+v %v", i, out, err)
		}
	}
	// Remove the restored native test copy before comparing source data.
	if err := os.RemoveAll(os.Getenv("CLAUDE_CONFIG_DIR")); err != nil {
		t.Fatal(err)
	}
	sessions.Reset()
	again, err := backup.AutoTick(at.Add(2*time.Hour), "test")
	if err != nil || again != nil {
		t.Fatalf("unchanged made %+v, %v", again, err)
	}
	backupTestThread(t, testThreadB, "added since previous backup", project)
	changed, err := backup.AutoTick(at.Add(4*time.Hour), "test")
	if err != nil || changed == nil || !changed.Sessions {
		t.Fatalf("session-only change: %+v %v", changed, err)
	}
	if _, err = os.Stat(companion(made.Name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pruning left companion: %v", err)
	}
	now, err := backup.AutoNow("test")
	if err != nil || !now.Sessions {
		t.Fatalf("immediate backup: %+v %v", now, err)
	}
	if err = backup.AutoDelete(changed.Name); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(companion(changed.Name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("delete left companion: %v", err)
	}
}

func TestScheduledWebDAVImportsAndSkipsReturnedSessions(t *testing.T) {
	fake := newSizeAwareDAV()
	server := httptest.NewServer(fake)
	defer server.Close()
	config := Config{URL: server.URL + "/dav", User: "me", Password: "pw", Passphrase: "automatic transfer", Sessions: true}
	source, dest, project := t.TempDir(), t.TempDir(), t.TempDir()
	ctx := context.Background()
	backupTestHome(t, source, config)
	backupTestThread(t, testThreadA, "shared through the schedule", project)
	if err := Now(ctx); err != nil {
		t.Fatal(err)
	}
	if a := SessionSyncActivity(); a.Latest == "" {
		t.Fatal("normal sync did not publish", a)
	}
	config.FollowServer = true
	backupTestHome(t, dest, config)
	if err := Now(ctx); err != nil {
		t.Fatal(err)
	}
	if items := sessions.BatchCandidates(); len(items) != 0 {
		t.Fatalf("manual choice triggered import: %v", items)
	}
	prefs := SessionPreferences{AutoImport: true, Targets: []string{"codex", "claude"}, All: true, Cwd: project}
	if err := SaveSessionPreferences(prefs); err != nil {
		t.Fatal(err)
	}
	if err := Now(ctx); err != nil {
		t.Fatal(err)
	}
	a := SessionSyncActivity()
	if a.ImportError != "" || a.Imported != 2 || a.ImportedAt.IsZero() {
		t.Fatalf("automatic import: %+v", a)
	}
	before := sessions.BatchCandidates()
	if len(before) != 2 {
		t.Fatalf("got %d native sessions", len(before))
	}
	fake.mu.Lock()
	previous := fake.gets
	fake.mu.Unlock()
	if err := Now(ctx); err != nil {
		t.Fatal(err)
	}
	if len(sessions.BatchCandidates()) != 2 {
		t.Fatal("second sync duplicated conversations")
	}
	fake.mu.Lock()
	after := fake.gets
	fake.mu.Unlock()
	// Ordinary setup sync can GET its own bundle. The inbox pass itself
	// must skip the completed snapshot before fetching its manifest.
	if n, err := receiveSessionBackups(ctx, config, prefs); err != nil || n != 0 {
		t.Fatalf("replay: %d %v", n, err)
	}
	fake.mu.Lock()
	gets := fake.gets
	fake.mu.Unlock()
	if gets != after {
		t.Fatalf("receipt fetched snapshot again: %d -> %d (initial %d)", after, gets, previous)
	}
	entries, _, err := sessions.CollectOutgoingBatch(ctx, nil, true)
	if err != nil || len(entries) != 0 {
		t.Fatalf("import echo: %d %v", len(entries), err)
	}
	var codex sessions.BatchCandidate
	for _, s := range before {
		if s.Agent == "codex" {
			codex = s
		}
	}
	files, _ := filepath.Glob(filepath.Join(os.Getenv("CODEX_HOME"), "sessions", "*", "*", "*", "*"+codex.ID+"*.jsonl"))
	if len(files) != 1 {
		t.Fatalf("native rollout: %d", len(files))
	}
	f, err := os.OpenFile(files[0], os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("{\"timestamp\":\"2026-11-26T12:00:00Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"edit on recipient\"}]}}\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	sessions.Reset()
	entries, _, err = sessions.CollectOutgoingBatch(ctx, nil, true)
	if err != nil || len(entries) != 1 {
		t.Fatalf("edited import not exportable: %d %v", len(entries), err)
	}
}

func TestLocalVersionsReuseSessionPieces(t *testing.T) {
	root, folder, project := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(root, "config"))
	t.Setenv("CODEX_HOME", filepath.Join(root, "codex"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(root, "claude"))
	sessions.Reset()
	config := backup.AutoConfig{Sessions: true, Passphrase: "local pool test", Keep: 7, Dir: folder}
	if err := backup.SaveAuto(config); err != nil {
		t.Fatal(err)
	}
	backupTestThread(t, testThreadA, "first text", project)
	backupTestThread(t, testThreadB, "other entry", project)
	first, err := backup.AutoNow("test")
	if err != nil {
		t.Fatal(err)
	}
	pool := filepath.Join(folder, backup.SessionPool)
	before, err := os.ReadDir(pool)
	if err != nil || len(before) != 2 {
		t.Fatal(len(before), err)
	}
	f, err := os.OpenFile(filepath.Join(os.Getenv("CODEX_HOME"), "sessions", "2026", "11", "25", "rollout-2026-11-25T10-00-00-"+testThreadA+".jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"additional reply\"}]}}\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	sessions.Reset()
	second, err := backup.AutoNow("test")
	if err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadDir(pool)
	if len(after) != 3 {
		t.Fatalf("append must add one object, got %d", len(after))
	}
	if err = backup.AutoDelete(first.Name); err != nil {
		t.Fatal(err)
	}
	after, _ = os.ReadDir(pool)
	if len(after) != 3 {
		t.Fatal("deleted shared pieces")
	}
	// Restore from the backup folder on another installation, without the writer's cache.
	dest := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dest, "config"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(dest, "claude"))
	t.Setenv("CODEX_HOME", filepath.Join(dest, "codex"))
	sessions.Reset()
	if err = backup.SaveAuto(config); err != nil {
		t.Fatal(err)
	}
	copies, err := RestoreAutoSessions(context.Background(), second.Name, "", project, []string{"codex"})
	if err != nil || len(copies) != 2 {
		t.Fatal(copies, err)
	}
	var appended bool
	for _, c := range copies {
		entries, e := sessions.CollectBatch(context.Background(), []sessions.BatchSource{{Agent: "codex", ID: c.Session}}, false)
		if e != nil || len(entries) != 1 {
			t.Fatal(entries, e)
		}
		for _, p := range entries[0].Parts {
			if p.Text == "additional reply" {
				appended = true
			}
		}
	}
	if !appended {
		t.Fatal("restore omitted appended turn")
	}
	if err = backup.AutoDelete(second.Name); err != nil {
		t.Fatal(err)
	}
	after, _ = os.ReadDir(pool)
	if len(after) != 0 {
		t.Fatal("unreferenced pool objects survived", len(after))
	}
}
