package davsync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/sessions"
)

func TestCurrentSessionAppendRestoresWholeThread(t *testing.T) {
	ctx := context.Background()
	fake := newSizeAwareDAV()
	var writes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" && !strings.HasSuffix(r.URL.Path, "manifest.sealed") {
			writes.Add(1)
		}
		fake.ServeHTTP(w, r)
	}))
	defer srv.Close()
	cfg := Config{URL: srv.URL + "/dav", User: "me", Password: "pw", Passphrase: "only a fixture"}
	backupTestHome(t, t.TempDir(), cfg)
	project := t.TempDir()
	backupTestThread(t, testThreadA, "original fixture message", project)
	backupTestThread(t, testThreadB, "unchanged conversation", project)
	initial, err := SyncLocalSessions(ctx)
	if err != nil || initial.Backup != currentSessionID || writes.Load() != 2 {
		t.Fatalf("initial: %+v %v", initial, err)
	}
	f := filepath.Join(os.Getenv("CODEX_HOME"), "sessions", "2026", "11", "25", "rollout-2026-11-25T10-00-00-"+testThreadA+".jsonl")
	file, err := os.OpenFile(f, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.WriteString("{\"timestamp\":\"2026-11-26T13:00:00Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"only the appended reply\"}]}}\n")
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	sessions.Reset()
	updated, err := SyncLocalSessions(ctx)
	if err != nil || updated.Backup != initial.Backup || writes.Load() != 3 {
		t.Fatalf("append: %+v %v, %d payload writes", updated, err, writes.Load())
	}
	backupTestHome(t, t.TempDir(), cfg)
	d, _ := newDAV(cfg)
	store := currentStore(cfg, d)
	head, err := store.readLibrary(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	var selected string
	var thread currentThread
	for k, t := range head.Threads {
		if t.Meta.Sources[0].ID == testThreadA {
			selected, thread = k, t
		}
	}
	if len(thread.Pieces) != 2 {
		t.Fatal("expected one initial piece and one append", thread)
	}
	delta, err := store.inThread(thread.Pieces[1].Collection).readEntry(ctx, batchManifest{}, thread.Pieces[1].ID)
	if err != nil || len(delta.Parts) != 1 || delta.Parts[0].Text != "only the appended reply" {
		t.Fatal("delta contains old history", delta, err)
	}
	restored, err := store.readThread(ctx, thread)
	if err != nil || len(restored.Parts) != 2 || restored.Parts[0].Text != "original fixture message" {
		t.Fatal(restored, err)
	}
	index, err := RestoreIndex(ctx, "webdav")
	if err != nil || len(index.Items) != 1 || !index.Items[0].Current {
		t.Fatal("one shared backup expected", index, err)
	}
	result, err := RunSessionBatch(ctx, SessionBatchRequest{Backups: []string{currentSessionID}, Entry: selected, Targets: []string{"claude"}, Cwd: project})
	if err != nil || len(result.Imports) != 1 || result.Imports[0].Error != "" {
		t.Fatal(result, err)
	}
}
func TestCurrentBackupConcurrentWriters(t *testing.T) {
	fake := newSizeAwareDAV()
	// Force both writers to construct a first manifest from an empty head.
	var put atomic.Int32
	gate := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "session_manifest.sealed") {
			switch put.Add(1) {
			case 1:
				select {
				case <-gate:
				case <-r.Context().Done():
					return
				}
			case 2:
				close(gate)
			}
		}
		fake.ServeHTTP(w, r)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg := Config{URL: server.URL + "/dav", User: "me", Password: "pw", Passphrase: "merge test"}
	backupTestHome(t, t.TempDir(), cfg)
	d, _ := newDAV(cfg)
	sources := []string{testThreadA, testThreadB}
	creation := time.Date(2026, time.November, 25, 10, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	for i, id := range sources {
		store := currentStore(cfg, d)
		store.localRoot = t.TempDir()
		plan := currentPlan{Unified: true, Changes: map[string]currentChange{}}
		for _, title := range []string{id, "shared conversation"} {
			c := sessions.BatchConversation{
				Sources: []sessions.BatchSource{{Agent: "codex", ID: id}}, Title: title,
				Start: creation, Last: creation.Add(time.Duration(i) * time.Minute),
				Parts: []sessions.Part{{Role: "user", Kind: "text", Text: title + " " + id}},
			}
			raw, _ := json.Marshal(c)
			thread := currentThread{Meta: c, Origin: id, Count: 1, Fingerprint: c.Fingerprint(), Revision: batchDigest(raw)}
			key := thread.identity()
			piece, err := store.stagePiece(ctx, sessions.BatchConversation{Parts: c.Parts})
			if err != nil {
				t.Fatal(err)
			}
			piece.Collection = key
			thread.Meta.Parts, thread.Pieces = nil, []currentPiece{piece}
			plan.Changes[key] = currentChange{Thread: thread}
		}
		wg.Go(func() {
			if _, err := store.commitPlan(ctx, plan); err != nil {
				t.Errorf("writer %d: %v", i, err)
			}
		})
	}
	wg.Wait()
	store := currentStore(cfg, d)
	head, err := store.readLibrary(ctx, true)
	if err != nil || len(head.Threads) != 3 {
		t.Fatal("one writer displaced another", len(head.Threads), err)
	}
	shared := currentThread{Meta: sessions.BatchConversation{Title: "shared conversation", Start: creation}}
	if winner := head.Threads[shared.identity()]; winner.Origin != testThreadB {
		t.Fatalf("older revision won the concurrent update: %+v", winner)
	}
}

func TestCurrentBackupMigratesDeviceManifests(t *testing.T) {
	server := httptest.NewServer(newSizeAwareDAV())
	defer server.Close()
	ctx := context.Background()
	cfg := Config{URL: server.URL + "/dav", User: "me", Password: "pw", Passphrase: "legacy migration"}
	backupTestHome(t, t.TempDir(), cfg)
	dav, err := newDAV(cfg)
	if err != nil {
		t.Fatal(err)
	}
	project := t.TempDir()
	backupTestThread(t, testThreadA, "earlier shared backup", project)
	legacy := currentStore(cfg, dav)
	legacy.headKey = batchDigest([]byte("legacy device"))
	plan, err := legacy.prepareLocalArchive(ctx, newSessionLibrary())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.commitPlan(ctx, plan); err != nil {
		t.Fatal(err)
	}
	backupTestHome(t, t.TempDir(), cfg)
	result, err := SyncLocalSessions(ctx)
	if err != nil || result.Conversations != 1 {
		t.Fatalf("migrating shared backup: %+v %v", result, err)
	}
	store := currentStore(cfg, dav)
	head, _, err := store.readHead(ctx)
	if err != nil || len(head.Threads) != 1 {
		t.Fatalf("unified catalog: %+v %v", head, err)
	}
	for key, thread := range head.Threads {
		if thread.Pieces[0].Collection != key {
			t.Fatalf("piece was not moved to its session directory: %+v", thread.Pieces)
		}
		entry, err := store.readThread(ctx, thread)
		if err != nil || len(entry.Parts) != 1 || entry.Parts[0].Text != "earlier shared backup" {
			t.Fatalf("migrated transcript: %+v %v", entry, err)
		}
	}
	if previous, _, err := legacy.readHead(ctx); err != nil || len(previous.Threads) != 1 {
		t.Fatalf("previous backup: %+v %v", previous, err)
	}
}

func TestRestoreIndexNeedsNoTranscriptReads(t *testing.T) {
	fake := newSizeAwareDAV()
	var metadata, gets atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
			metadata.Add(1)
		}
		if r.Method == "GET" {
			gets.Add(1)
		}
		fake.ServeHTTP(w, r)
	}))
	defer srv.Close()
	cfg := Config{URL: srv.URL + "/dav", User: "me", Password: "pw", Passphrase: "index test"}
	backupTestHome(t, t.TempDir(), cfg)
	backupTestThread(t, testThreadA, "one", t.TempDir())
	if _, err := SyncLocalSessions(context.Background()); err != nil {
		t.Fatal(err)
	}
	metadata.Store(0)
	gets.Store(0)
	local, err := RestoreIndex(context.Background(), "local")
	if err != nil || len(local.Items) != 1 || gets.Load() != 0 || metadata.Load() != 0 {
		t.Fatal(local, err, gets.Load(), metadata.Load())
	}
	page, err := RestoreIndex(context.Background(), "webdav")
	if err != nil || len(page.Items) != 1 || gets.Load() != 0 || metadata.Load() != 3 {
		t.Fatal(page, err, gets.Load(), metadata.Load())
	}
}

func TestCatalogRemoteDirectoriesOverlap(t *testing.T) {
	fake := newSizeAwareDAV()
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PROPFIND" {
			started <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		fake.ServeHTTP(w, r)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := Config{URL: srv.URL + "/dav", User: "me", Password: "pw", Passphrase: "index test"}
	backupTestHome(t, t.TempDir(), cfg)
	done := make(chan error, 1)
	go func() { _, err := RestoreIndex(ctx, "webdav"); done <- err }()
	for range 3 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("one directory request blocked another")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
