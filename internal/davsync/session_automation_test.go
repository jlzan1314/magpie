package davsync

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/backup"
	"github.com/yetone/magpie/internal/filelock"
	"github.com/yetone/magpie/internal/sessions"
	"github.com/yetone/magpie/internal/steady"
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
	t.Setenv("HOME", dest)
	t.Setenv("USERPROFILE", dest)
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
	prefs := SessionPreferences{AutoImport: true, Targets: []string{"codex", "claude"}}
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
	if err := SetFollowServer(false); err != nil {
		t.Fatal(err)
	}
	if err := DeleteSessionBackup(ctx, currentSessionID, "webdav"); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := SyncLocalSessions(ctx)
	if err != nil || rebuilt.Conversations != 1 || rebuilt.Duplicates != 1 || !rebuilt.Remote {
		t.Fatalf("rebuilding from imported copies: %+v %v", rebuilt, err)
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

func TestScheduledWebDAVCanceledImportDoesNotReportFailure(t *testing.T) {
	fake := newSizeAwareDAV()
	sessionCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var listings atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
			listings.Add(1)
			cancel()
			w.WriteHeader(http.StatusForbidden)
			return
		}
		fake.ServeHTTP(w, r)
	}))
	defer server.Close()
	config := Config{URL: server.URL + "/dav", User: "me", Password: "pw", Passphrase: "canceled session import"}
	backupTestHome(t, t.TempDir(), config)
	if err := SaveSessionPreferences(SessionPreferences{AutoImport: true, Targets: []string{"codex"}}); err != nil {
		t.Fatal(err)
	}
	if err := syncNow(context.Background(), sessionCtx, false, ""); err != nil {
		t.Fatal(err)
	}
	if listings.Load() == 0 || !errors.Is(sessionCtx.Err(), context.Canceled) {
		t.Fatal("test did not cancel an active session import")
	}
	if got := loadState().Sessions.ImportError; got != "" {
		t.Fatalf("canceled import saved an error: %q", got)
	}
	if err := Now(context.Background()); err != nil {
		t.Fatal(err)
	}
	if activity := SessionSyncActivity(); !strings.Contains(activity.ImportError, "HTTP 403") {
		t.Fatalf("real import failure was lost: %+v", activity)
	}
	if err := syncNow(context.Background(), sessionCtx, false, ""); err != nil {
		t.Fatal(err)
	}
	if activity := SessionSyncActivity(); !strings.Contains(activity.ImportError, "HTTP 403") {
		t.Fatalf("skipping an import on a canceled sync hid the last real failure: %+v", activity)
	}
}

func TestSessionActivityDropsStoredImportCancellation(t *testing.T) {
	config := Config{URL: "http://127.0.0.1/dav", User: "me", Password: "pw", Passphrase: "stored session import status"}
	backupTestHome(t, t.TempDir(), config)
	status := state{Key: stateKey(config), Sessions: &sessionSyncState{Imported: 3, ImportedAt: time.Now()}}
	job, _ := beginSessionJob(config)
	t.Cleanup(func() { job.finish(SessionBatchResult{}, nil) })
	ctx := context.WithValue(context.Background(), sessionJobKey{}, job)
	updateSessionActivity(ctx, func(a *SessionActivity) { a.Phase, a.Objects, a.Count = "uploading", 231, 2476 })
	for _, test := range []struct{ stored, want string }{
		{"context canceled", ""},
		{"reading WebDAV object: context canceled\ncontext canceled", ""},
		{"reading WebDAV object: context canceled\nlisting session backups on WebDAV: HTTP 403", "listing session backups on WebDAV: HTTP 403"},
		{"reading WebDAV object: context deadline exceeded", "reading WebDAV object: context deadline exceeded"},
	} {
		status.Sessions.ImportError = test.stored
		if err := saveState(status); err != nil {
			t.Fatal(err)
		}
		activity := SessionSyncActivity()
		if activity.ImportError != test.want || activity.Phase != "uploading" || activity.Objects != 231 || activity.Count != 2476 || activity.Imported != 3 || activity.ImportedAt.IsZero() {
			t.Errorf("stored %q, want import error %q and intact upload progress and import history: %+v", test.stored, test.want, activity)
		}
	}
}

func TestScheduledWebDAVImportsOnlyRecentSessions(t *testing.T) {
	skipSessionFixtureFlushes(t)
	fake := newSizeAwareDAV()
	var downloads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/current/threads/") {
			downloads.Add(1)
		}
		fake.ServeHTTP(w, r)
	}))
	defer server.Close()
	config := Config{URL: server.URL + "/dav", User: "me", Password: "pw", Passphrase: "recent sessions"}
	source, dest := t.TempDir(), t.TempDir()
	t.Setenv("HOME", dest)
	t.Setenv("USERPROFILE", dest)
	ctx := context.Background()
	prefs := SessionPreferences{AutoImport: true, Targets: []string{"codex"}}
	dav, err := newDAV(config)
	if err != nil {
		t.Fatal(err)
	}
	publish := func(entries []sessions.BatchConversation) {
		t.Helper()
		backupTestHome(t, source, config)
		if _, err := currentStore(config, dav).updateCurrent(ctx, SessionBatchRequest{}, entries, nil); err != nil {
			t.Fatal(err)
		}
	}
	receive := func(want int) {
		t.Helper()
		backupTestHome(t, dest, config)
		if n, err := receiveSessionBackups(ctx, config, prefs); err != nil || n != want {
			t.Fatalf("imported %d sessions, wanted %d: %v", n, want, err)
		}
		sessions.Reset()
	}
	previous := recentSyncFixture(0)
	publish([]sessions.BatchConversation{previous})
	receive(1)
	before := sessions.BatchCandidates()
	if len(before) != 1 {
		t.Fatalf("first sync left %d native sessions", len(before))
	}

	previous.Last = previous.Start.Add(time.Second)
	previous.Parts = append(previous.Parts, sessions.Part{Role: "assistant", Kind: "text", Text: "outside the window"})
	entries := []sessions.BatchConversation{previous}
	for index := 1; index <= 201; index++ {
		entries = append(entries, recentSyncFixture(index))
	}
	entries[2].Last = time.Time{}
	publish(entries)
	backupTestHome(t, dest, config)
	rememberSessionRevisions(t, entries[3:201])
	downloads.Store(0)
	receive(2)
	if got := downloads.Load(); got != 2 {
		t.Fatalf("downloaded %d session payloads, wanted the two unseen revisions inside the 200-session window", got)
	}
	candidates := sessions.BatchCandidates()
	if len(candidates) != 3 {
		t.Fatalf("unexpected session window: %d native sessions", len(candidates))
	}
	for _, candidate := range candidates {
		if candidate.Title == entries[1].Title {
			t.Fatal("a session outside the window displaced a newer creation without activity")
		}
	}
	old, _, err := sessions.CollectAvailableBatch(ctx, []sessions.BatchSource{before[0].BatchSource}, false)
	if err != nil || len(old) != 1 || len(old[0].Parts) != 1 {
		t.Fatalf("sync changed a previously imported session outside the window: %v", err)
	}
	downloads.Store(0)
	receive(0)
	if got := downloads.Load(); got != 0 {
		t.Fatalf("unchanged sync downloaded %d payloads or backfilled older sessions", got)
	}
}

func recentSyncFixture(index int) sessions.BatchConversation {
	stamp := time.Date(2026, time.November, 25, 10, index, 0, 0, time.UTC)
	title := fmt.Sprintf("Remote conversation %03d", index)
	return sessions.BatchConversation{
		Sources: []sessions.BatchSource{{Agent: "codex", ID: fmt.Sprintf("0196ce3b-81fc-7000-8000-%012x", index)}},
		Title:   title,
		Start:   stamp,
		Last:    stamp,
		Parts:   []sessions.Part{{Role: "user", Kind: "text", Text: title}},
	}
}

func TestScheduledWebDAVLegacyWindowDoesNotBackfill(t *testing.T) {
	skipSessionFixtureFlushes(t)
	fake := newSizeAwareDAV()
	server := httptest.NewServer(fake)
	defer server.Close()
	config := Config{URL: server.URL + "/dav", User: "me", Password: "pw", Passphrase: "recent legacy sessions"}
	source, dest := t.TempDir(), t.TempDir()
	t.Setenv("HOME", dest)
	t.Setenv("USERPROFILE", dest)
	ctx := context.Background()
	prefs := SessionPreferences{AutoImport: true, Targets: []string{"codex"}}
	backupTestHome(t, dest, config)
	if n, err := receiveSessionBackups(ctx, config, prefs); err != nil || n != 0 {
		t.Fatalf("empty WebDAV sync imported %d sessions: %v", n, err)
	}
	backupTestHome(t, source, config)
	dav, err := newDAV(config)
	if err != nil {
		t.Fatal(err)
	}
	store := batchStore{dav: dav, passphrase: config.Passphrase}
	var entries []sessions.BatchConversation
	for index := 0; index < 202; index++ {
		entries = append(entries, recentSyncFixture(index))
	}
	if _, stats, err := store.save(ctx, entries, true, batchPartSize); err != nil || len(stats.failed) != 0 {
		t.Fatalf("publishing legacy fixture: %v", err)
	}
	backupTestHome(t, dest, config)
	rememberSessionRevisions(t, entries[:198])
	if n, err := receiveSessionBackups(ctx, config, prefs); err != nil || n != 2 {
		t.Fatalf("legacy sync imported %d sessions, wanted the two unseen revisions inside the window: %v", n, err)
	}
	if n, err := receiveSessionBackups(ctx, config, prefs); err != nil || n != 0 {
		t.Fatalf("legacy sync backfilled %d sessions: %v", n, err)
	}
}

func rememberSessionRevisions(t *testing.T, entries []sessions.BatchConversation) {
	t.Helper()
	files, err := filepath.Glob(path("session-inbox-*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("found %d session receipt files: %v", len(files), err)
	}
	var receipts inboxReceipts
	raw, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &receipts); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		raw, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		receipts.Entries[batchDigest(raw)] = true
	}
	raw, err = json.Marshal(receipts)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files[0], raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func skipSessionFixtureFlushes(t *testing.T) {
	t.Helper()
	flush := steady.Sync
	steady.Sync = func(*os.File) error { return nil }
	t.Cleanup(func() { steady.Sync = flush })
}

func TestScheduledWebDAVOverwritesSharedSessionAndKeepsCreationTime(t *testing.T) {
	server := httptest.NewServer(newSizeAwareDAV())
	defer server.Close()
	config := Config{URL: server.URL + "/dav", User: "me", Password: "pw", Passphrase: "automatic revisions", Sessions: true}
	source, dest, project := t.TempDir(), t.TempDir(), t.TempDir()
	t.Setenv("HOME", dest)
	t.Setenv("USERPROFILE", dest)
	ctx := context.Background()
	prefs := SessionPreferences{AutoImport: true, Targets: []string{"codex", "claude"}}
	creation := time.Date(2026, time.November, 25, 10, 0, 0, 123456789, time.UTC)
	publish := func(id, stamp, text string) {
		t.Helper()
		backupTestHome(t, source, config)
		backupTestThread(t, id, "starting question", project)
		file := filepath.Join(os.Getenv("CODEX_HOME"), "sessions", "2026", "11", "25", "rollout-2026-11-25T10-00-00-"+id+".jsonl")
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(strings.ReplaceAll(string(raw), "2026-11-25T10:00:00Z", creation.Format(time.RFC3339Nano))), 0600); err != nil {
			t.Fatal(err)
		}
		if text != "" {
			f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.NewEncoder(f).Encode(map[string]any{"type": "response_item", "timestamp": stamp, "payload": map[string]any{
				"type": "message", "role": "assistant", "content": []map[string]string{{"type": "output_text", "text": text}},
			}}); err != nil {
				f.Close()
				t.Fatal(err)
			}
			if err = f.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := SyncLocalSessions(ctx); err != nil {
			t.Fatal(err)
		}
	}
	receive := func() []sessions.BatchCandidate {
		t.Helper()
		backupTestHome(t, dest, config)
		if _, err := receiveSessionBackups(ctx, config, prefs); err != nil {
			t.Fatal(err)
		}
		sessions.Reset()
		return sessions.BatchCandidates()
	}
	publish(testThreadA, "", "")
	initial := receive()
	if len(initial) != 2 {
		t.Fatalf("initial import: %d sessions", len(initial))
	}
	ids := map[string]string{}
	for _, item := range initial {
		ids[item.Agent] = item.ID
		if native, ok := sessions.Find(item.Agent, item.ID); !ok || native.Cwd != filepath.Join(dest, "sessions") {
			t.Fatalf("%s imported into %q", item.Agent, native.Cwd)
		}
	}
	publish(testThreadA, "2026-11-25T10:01:00Z", "only the appended answer")
	backupTestHome(t, dest, config)
	locks := filepath.Join(os.Getenv("CODEX_HOME"), "thread-writer-locks")
	if err := os.MkdirAll(locks, 0o700); err != nil {
		t.Fatal(err)
	}
	release, ok := filelock.Lock(filepath.Join(locks, ids["codex"]+".lock"), filelock.Wait)
	if !ok {
		t.Fatal("could not hold the Codex writer lock")
	}
	_, blocked := receiveSessionBackups(ctx, config, prefs)
	release()
	if blocked == nil {
		t.Fatal("automatic import advanced a session held open by Codex")
	}
	if entries, _, err := sessions.CollectOutgoingBatch(ctx, nil, true); err != nil || len(entries) != 0 {
		t.Fatalf("blocked import echoed back: %d %v", len(entries), err)
	}
	check := func(text string) {
		t.Helper()
		updated := receive()
		if len(updated) != 2 {
			t.Fatalf("update created a new native session: %d sessions", len(updated))
		}
		for _, item := range updated {
			if item.ID != ids[item.Agent] {
				t.Fatalf("%s session ID changed: %s", item.Agent, item.ID)
			}
			if native, ok := sessions.Find(item.Agent, item.ID); !ok || !native.Start.Equal(creation) {
				t.Fatalf("%s native creation time: %s", item.Agent, native.Start)
			}
			entries, err := sessions.CollectBatch(ctx, []sessions.BatchSource{item.BatchSource}, false)
			if err != nil || len(entries) != 1 || len(entries[0].Parts) != 2 || entries[0].Parts[1].Text != text || !entries[0].Start.Equal(creation) {
				t.Fatalf("%s import: %+v %v", item.Agent, entries, err)
			}
		}
		if entries, _, err := sessions.CollectOutgoingBatch(ctx, nil, true); err != nil || len(entries) != 0 {
			t.Fatalf("updated import echoed back: %d %v", len(entries), err)
		}
	}
	check("only the appended answer")
	publish(testThreadB, "2026-11-25T10:02:00Z", "a replacement from another native ID")
	check("a replacement from another native ID")
	dav, _ := newDAV(config)
	head, err := currentStore(config, dav).readLibrary(ctx, true)
	if err != nil || len(head.Threads) != 1 {
		t.Fatalf("expected one shared session: %d %v", len(head.Threads), err)
	}
	for key, thread := range head.Threads {
		if len(thread.Pieces) != 1 || thread.Pieces[0].Collection != key {
			t.Fatalf("replacement escaped the original session directory: %+v", thread.Pieces)
		}
	}

	applyHome := t.TempDir()
	t.Setenv("HOME", applyHome)
	t.Setenv("USERPROFILE", applyHome)
	backupTestHome(t, applyHome, config)
	localProject := t.TempDir()
	backupTestThread(t, testThreadC, "starting question", localProject)
	native, ok := sessions.Find("codex", testThreadC)
	if !ok {
		t.Fatal("missing native target session")
	}
	raw, err := os.ReadFile(native.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(native.Path, bytes.ReplaceAll(raw, []byte("2026-11-25T10:00:00Z"), []byte(creation.Format(time.RFC3339Nano))), 0600); err != nil {
		t.Fatal(err)
	}
	sessions.Reset()
	result, err := RunSessionBatch(ctx, SessionBatchRequest{Current: true, Targets: []string{"codex"}})
	if err != nil || result.Conversations != 1 || len(result.Imports) != 1 || result.Imports[0].Error != "" || result.Imports[0].Session != testThreadC {
		t.Fatalf("merge and apply to an existing native session: %+v %v", result, err)
	}
	sessions.Reset()
	if items := sessions.BatchCandidates(); len(items) != 1 || items[0].Cwd != localProject {
		t.Fatalf("applying to one target created another session: %+v", items)
	}
	if updated, ok := sessions.Find("codex", testThreadC); !ok || !updated.Start.Equal(creation) {
		t.Fatalf("native target creation time: %s", updated.Start)
	}
	entries, err := sessions.CollectBatch(ctx, []sessions.BatchSource{{Agent: "codex", ID: testThreadC}}, false)
	if err != nil || len(entries) != 1 || len(entries[0].Parts) != 2 || entries[0].Parts[1].Text != "a replacement from another native ID" {
		t.Fatalf("native target was not updated: %+v %v", entries, err)
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
