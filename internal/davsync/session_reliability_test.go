package davsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/sessions"
)

const testThreadA = "0196ce3b-81fc-7000-8000-7d8089eb9821"
const testThreadB = "0196ce3b-81fc-7000-8000-7d8089eb9822"
const testThreadC = "0196ce3b-81fc-7000-8000-7d8089eb9823"

func backupTestHome(t *testing.T, home string, c Config) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, "claude"))
	sessions.Reset()
	if err := Configure(c); err != nil {
		t.Fatal(err)
	}
}

func backupTestThread(t *testing.T, id, text, project string) {
	t.Helper()
	const stamp = "2026-11-25T10:00:00Z"
	file := filepath.Join(os.Getenv("CODEX_HOME"), "sessions", "2026", "11", "25", "rollout-2026-11-25T10-00-00-"+id+".jsonl")
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	data := fmt.Sprintf(`{"timestamp":%q,"type":"session_meta","payload":{"id":%q,"cwd":%q,"timestamp":%q}}
{"timestamp":%q,"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":%q}]}}
`, stamp, id, project, stamp, stamp, text)
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func awaitSessionJob(t *testing.T, predicate func(SessionActivity) bool) SessionActivity {
	t.Helper()
	end := time.Now().Add(15 * time.Second)
	for {
		v := SessionSyncActivity()
		if predicate(v) {
			return v
		}
		if time.Now().After(end) {
			t.Fatalf("unexpected session activity: %+v", v)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSessionJobLostReplyAndStrongReceipts(t *testing.T) {
	fake := newSizeAwareDAV()
	var lose atomic.Bool
	lose.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" && lose.CompareAndSwap(true, false) {
			// Commit the object, then lose its acknowledgement.
			fake.ServeHTTP(httptest.NewRecorder(), r)
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fake.ServeHTTP(w, r)
	}))
	defer srv.Close()
	cfg := Config{URL: srv.URL + "/dav", Passphrase: "receipt test", User: "me", Password: "pw", Sessions: true}
	backupTestHome(t, t.TempDir(), cfg)
	backupTestThread(t, testThreadA, "an example", t.TempDir())
	if _, err := StartSessionSync(SessionRetry{}); err != nil {
		t.Fatal(err)
	}
	waiting := awaitSessionJob(t, func(s SessionActivity) bool { return s.Phase == "waiting" })
	if waiting.CanRetry || waiting.RetryAt.IsZero() {
		t.Fatalf("server retry time was not respected: %+v", waiting)
	}
	if _, err := StartSessionSync(SessionRetry{}); err != nil {
		t.Fatal(err)
	}
	done := awaitSessionJob(t, func(s SessionActivity) bool { return s.Phase == "done" })
	if done.Latest == "" || done.Total == 0 || done.Bytes != done.Total || done.Skipped != 1 || done.Uploaded != 1 {
		t.Fatalf("bad progress: %+v", done)
	}
	fake.mu.Lock()
	puts := fake.puts
	for p, raw := range fake.files {
		if strings.HasSuffix(p, "manifest.sealed") {
			continue
		}
		fake.files[p] = make([]byte, len(raw))
		fake.etags[p] = `"substituted"`
	}
	fake.mu.Unlock()
	if puts != 2 {
		t.Fatalf("lost acknowledgement caused %d PUTs", puts)
	}
	updated, err := SyncLocalSessions(context.Background())
	if err != nil || !updated.Remote || updated.Uploaded != 1 {
		t.Fatalf("same-size substitution: %+v, %v", updated, err)
	}
}

func TestSessionPartialBackupRestoresOnOtherDevice(t *testing.T) {
	fake := newSizeAwareDAV()
	var reject atomic.Value
	reject.Store("")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if leaf := reject.Load().(string); leaf != "" && r.Method == "PUT" && strings.HasSuffix(r.URL.Path, leaf) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		fake.ServeHTTP(w, r)
	}))
	defer srv.Close()
	cfg := Config{URL: srv.URL + "/dav", Passphrase: "partial test", User: "me", Password: "pw", Sessions: true}
	first, second, project := t.TempDir(), t.TempDir(), t.TempDir()
	backupTestHome(t, first, cfg)
	backupTestThread(t, testThreadA, "one good session", project)
	backupTestThread(t, testThreadB, "second good session", project)
	backupTestThread(t, testThreadC, "rejected session", project)
	items, err := sessions.CollectBatch(context.Background(), []sessions.BatchSource{{Agent: "codex", ID: testThreadC}}, false)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(sessions.BatchConversation{Parts: items[0].Parts})
	if err != nil {
		t.Fatal(err)
	}
	reject.Store("session_" + batchDigest(raw) + ".sealed")
	result, err := SyncLocalSessions(context.Background())
	if err != nil || !result.Remote || result.Conversations != 2 || len(result.Failed) != 1 || result.Failed[0].Source.ID != testThreadC {
		t.Fatalf("partial upload: %+v, %v", result, err)
	}
	cfg.FollowServer = true
	backupTestHome(t, second, cfg)
	page, err := RestoreChoices(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, item := range page.Items {
		if item.ID != result.Backup {
			if item.Ready {
				t.Fatal("uncommitted backup offered for restore")
			}
			continue
		}
		found = true
		if !item.RemoteReady || item.Device == "" || len(item.Entries) != 2 {
			t.Fatalf("restore choice: %+v", item)
		}
		target := item.Entries[0]
		restored, err := RunSessionBatch(context.Background(), SessionBatchRequest{Backups: []string{item.ID}, Entry: target.ID, Targets: []string{"codex"}, Cwd: project})
		if err != nil || len(restored.Imports) != 1 || restored.Imports[0].Error != "" {
			t.Fatalf("selective restore: %+v, %v", restored, err)
		}
		entries, err := sessions.CollectBatch(context.Background(), []sessions.BatchSource{{Agent: "codex", ID: restored.Imports[0].Session}}, false)
		if err != nil || len(entries) != 1 || len(entries[0].Parts) == 0 {
			t.Fatalf("restored transcript unreadable: %v", err)
		}
		if !strings.Contains(entries[0].Parts[0].Text, "good session") {
			t.Fatalf("wrong restored content: %+v", entries[0].Parts)
		}
		d, _ := newDAV(cfg)
		cache := currentStore(cfg, d)
		head, err := cache.readLibrary(context.Background(), true)
		if err != nil {
			t.Fatal(err)
		}
		piece := head.Threads[target.ID].Pieces[0]
		if err := os.WriteFile(cache.filename("conversations", piece.ID), []byte("damaged fixture"), 0600); err != nil {
			t.Fatal(err)
		}
		again, err := RunSessionBatch(context.Background(), SessionBatchRequest{Backups: []string{item.ID}, Entry: target.ID, Targets: []string{"codex"}, Cwd: project})
		if err != nil || len(again.Imports) != 1 || !again.Imports[0].Existing {
			t.Fatalf("verified remote did not repair local cache: %+v, %v", again, err)
		}

	}
	if !found {
		t.Fatal("verified partial backup missing from receiver")
	}
	cfg.FollowServer = false
	backupTestHome(t, first, cfg)
	reject.Store("")
	if _, err := StartSessionSync(SessionRetry{Source: &sessions.BatchSource{Agent: "codex", ID: testThreadC}}); err != nil {
		t.Fatal(err)
	}
	retried := awaitSessionJob(t, func(a SessionActivity) bool { return a.Phase == "done" })
	if retried.Conversations != 3 || len(retried.Failed) != 0 {
		t.Fatalf("targeted retry: %+v", retried)
	}
}

// The subprocess uses only the fixture paths created by its parent test.
func TestSessionProcessHelper(t *testing.T) {
	dir := os.Getenv("MAGPIE_SESSION_FIXTURE_DIR")
	if dir == "" {
		t.Skip("subprocess fixture")
	}
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "config"))
	t.Setenv("CODEX_HOME", filepath.Join(dir, "codex"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(dir, "claude"))
	switch os.Getenv("MAGPIE_SESSION_FIXTURE_MODE") {
	case "start":
		_, err := SyncLocalSessions(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	case "restart":
		ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		done := make(chan struct{})
		go func() { run(ctx, 0); close(done) }()
		for ctx.Err() == nil {
			st := loadState()
			if st.Sessions != nil && st.Sessions.Pending == "" && st.Sessions.Backup != "" {
				stop()
				<-done
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		<-done
		t.Fatal("prepared manual backup did not resume after process restart")
	default:
		t.Fatal("unknown fixture mode")
	}
}

func TestSessionBackupSurvivesProcessExit(t *testing.T) {
	fake := newSizeAwareDAV()
	pause := make(chan struct{})
	release := make(chan struct{})
	var puts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" && puts.Add(1) == 2 {
			close(pause)
			<-release
			return
		}
		fake.ServeHTTP(w, r)
	}))
	defer srv.Close()
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	c := Config{URL: srv.URL + "/dav", User: "me", Password: "pw", Passphrase: "process resume test", AutoEvery: Manual}
	dir := t.TempDir()
	backupTestHome(t, dir, c)
	backupTestThread(t, testThreadA, "first fixture", "/example")
	backupTestThread(t, testThreadB, "second fixture", "/example")
	child := func(mode string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSessionProcessHelper$")
		cmd.Env = append(os.Environ(), "MAGPIE_SESSION_FIXTURE_DIR="+dir, "MAGPIE_SESSION_FIXTURE_MODE="+mode)
		return cmd
	}
	first := child("start")
	var log bytes.Buffer
	first.Stdout, first.Stderr = &log, &log
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pause:
	case <-time.After(20 * time.Second):
		first.Process.Kill()
		first.Wait()
		t.Fatalf("first process did not start transfer: %s", log.String())
	}
	first.Process.Kill()
	first.Wait()
	unblock()
	st := loadState()
	if st.Sessions == nil || !st.Sessions.Explicit || !validBatchID(st.Sessions.Pending) {
		t.Fatalf("missing durable request: %+v", st.Sessions)
	}
	id := st.Sessions.Pending
	if out, err := child("restart").CombinedOutput(); err != nil {
		t.Fatalf("second process: %v: %s", err, out)
	}
	st = loadState()
	if st.Sessions.Backup != currentSessionID || !validBatchID(id) || st.Sessions.Skipped != 1 || st.Sessions.Uploaded != 2 {
		t.Fatalf("resumed a different snapshot or retransmitted a verified file: %+v", st.Sessions)
	}
}

func TestSessionCaptureContinuesPastUnavailableSource(t *testing.T) {
	fake := newSizeAwareDAV()
	server := httptest.NewServer(fake)
	defer server.Close()
	config := Config{URL: server.URL + "/dav", User: "me", Password: "pw", Passphrase: "capture test"}
	backupTestHome(t, t.TempDir(), config)
	project := t.TempDir()
	backupTestThread(t, testThreadB, "readable sibling", project)
	result, err := RunSessionBatch(context.Background(), SessionBatchRequest{
		Sources: []sessions.BatchSource{{Agent: "codex", ID: testThreadA}, {Agent: "codex", ID: testThreadB}},
		Targets: []string{"claude"}, Cwd: project,
	})
	if err != nil || !result.Remote || result.Conversations != 1 || len(result.Imports) != 1 || result.Imports[0].Error != "" || len(result.Failed) != 1 || result.Failed[0].Source.ID != testThreadA {
		t.Fatalf("capture did not isolate the unavailable source: %+v, %v", result, err)
	}
}
