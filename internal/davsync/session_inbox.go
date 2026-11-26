package davsync

import (
	"context"
	"encoding/json"
	"errors"

	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/yetone/magpie/internal/filelock"
	"github.com/yetone/magpie/internal/sessions"
	"github.com/yetone/magpie/internal/steady"
)

// Independent of hostname, which two machines (especially WSL) may share.
func sessionDeviceID() (string, error) {
	name := path("session-device-id")
	read := func() (string, error) {
		data, err := os.ReadFile(name)
		if err != nil {
			return "", err
		}
		id, err := uuid.Parse(strings.TrimSpace(string(data)))
		return id.String(), err
	}
	if id, err := read(); !errors.Is(err, fs.ErrNotExist) {
		return id, err
	}
	if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
		return "", err
	}
	unlock, ok := filelock.Lock(name+".lock", filelock.Wait)
	if !ok {
		return "", errors.New("session device identity is busy")
	}
	defer unlock()
	if id, err := read(); !errors.Is(err, fs.ErrNotExist) {
		return id, err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), steady.WriteFile(name, []byte(id.String()), 0600)
}

type inboxReceipts struct {
	Started bool            `json:"started"`
	Backups map[string]bool `json:"backups"`
	Entries map[string]bool `json:"entries"`
}

func receiveSessionBackups(ctx context.Context, c Config, p SessionPreferences) (int, error) {
	targets := slices.Clone(p.Targets)
	slices.Sort(targets)
	if err := sessions.CheckBatchTargets(targets, p.Cwd); err != nil {
		return 0, err
	}
	own, err := sessionDeviceID()
	if err != nil {
		return 0, err
	}
	host, err := os.Hostname()
	if err != nil {
		return 0, err
	}
	d, err := newDAV(c)
	if err != nil {
		return 0, err
	}
	ctx = withSessionDAVTimeouts(ctx)
	list, err := listSessionBackups(ctx, c, d)
	if err != nil {
		return 0, err
	}
	if list.WebDAVError != "" {
		return 0, errors.New(list.WebDAVError)
	}
	scope := sum([]byte(stateKey(c) + "\x00" + strings.Join(targets, ",") + "\x00" + p.Cwd))
	file := path("session-inbox-" + scope + ".json")
	receipts := inboxReceipts{Backups: map[string]bool{}, Entries: map[string]bool{}}
	data, err := os.ReadFile(file)
	if err == nil {
		if err = json.Unmarshal(data, &receipts); err != nil {
			return 0, err
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return 0, err
	}
	if receipts.Backups == nil || receipts.Entries == nil {
		return 0, errors.New("invalid session import receipts")
	}
	// Restoring an old backup updates its local mtime. Use the server's
	// collection date for initial selection, keeping ties and undated rows.
	slices.SortStableFunc(list.Items, func(a, b SessionBackup) int { return b.remoteTime.Compare(a.remoteTime) })
	newest := map[string]time.Time{}
	var receiver *sessions.BatchReceiver
	dirty := false
	imported := 0
	var problems []error
	hasCurrent := slices.ContainsFunc(list.Items, func(item SessionBackup) bool { return item.Current && item.WebDAV })
	for _, item := range list.Items {
		if ctx.Err() != nil {
			problems = append(problems, ctx.Err())
			break
		}
		if item.Current && item.WebDAV {
			cur := currentStore(c, d)
			h, e := cur.readLibrary(ctx, true)
			if e != nil {
				problems = append(problems, e)
				continue
			}
			for _, t := range h.Threads {
				if ctx.Err() != nil {
					problems = append(problems, ctx.Err())
					break
				}
				if t.Origin == own || receipts.Entries[t.Revision] {
					continue
				}
				entry, e := cur.readThread(ctx, t)
				if e == nil && receiver == nil {
					receiver, e = sessions.NewBatchReceiver(ctx, targets, p.Cwd)
				}
				if e == nil {
					var n int
					n, e = receiver.Receive(ctx, entry)
					imported += n
				}
				if e != nil {
					problems = append(problems, e)
					continue
				}
				receipts.Entries[t.Revision] = true
				dirty = true
			}
			continue
		}
		if hasCurrent || !item.WebDAV || receipts.Backups[item.ID] {
			continue
		}
		store := batchStore{dav: d, passphrase: c.Passphrase, batchID: item.ID}
		// An upload can have a collection before its verified manifest.
		raw, e := store.remote(ctx, "backups", item.ID)
		if errors.Is(e, errSessionBackupMissing) {
			continue
		}
		var m batchManifest
		if e == nil {
			e = store.open(raw, item.ID, &m)
		}
		if e != nil {
			problems = append(problems, e)
			continue
		}
		if !m.valid(2) {
			problems = append(problems, errors.New("invalid session backup manifest"))
			continue
		}
		source := m.Origin
		if source == "" {
			source = m.Device
		}
		foreign := source != "" && source != own && !(m.Origin == "" && m.Device == host)
		if foreign && (receipts.Started || item.remoteTime.IsZero() || !item.remoteTime.Before(newest[source])) {
			complete := true
			for _, key := range m.Entries {
				if receipts.Entries[key] {
					continue
				}
				if ctx.Err() != nil {
					complete = false
					break
				}
				entry, e := store.readEntry(ctx, m, key)
				if e == nil && receiver == nil {
					receiver, e = sessions.NewBatchReceiver(ctx, targets, p.Cwd)
				}
				if e == nil {
					var n int
					n, e = receiver.Receive(ctx, entry)
					imported += n
				}
				if e != nil {
					problems = append(problems, e)
					complete = false
					continue
				}
				receipts.Entries[key] = true
				dirty = true
			}
			if !complete {
				continue
			}
			if item.remoteTime.After(newest[source]) {
				newest[source] = item.remoteTime
			}
		}
		receipts.Backups[item.ID] = true
		dirty = true
	}
	if !receipts.Started && len(problems) == 0 {
		receipts.Started = true
		dirty = true
	}
	if dirty {
		data, e := json.Marshal(receipts)
		if e == nil {
			e = steady.WriteFile(file, data, 0600)
		}
		problems = append(problems, e)
	}
	return imported, errors.Join(problems...)
}
