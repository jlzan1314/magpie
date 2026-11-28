package davsync

import (
	"context"
	"errors"
	"io/fs"
	"os"
)

type restoreEntry struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type RestoreChoice struct {
	SessionBackup
	Device      string         `json:"device,omitempty"`
	Entries     []restoreEntry `json:"entries,omitempty"`
	RemoteReady bool           `json:"remoteReady,omitempty"`
	Ready       bool           `json:"ready"`
	Problem     string         `json:"problem,omitempty"`
}

type RestorePage struct {
	Items           []RestoreChoice `json:"items"`
	More            bool            `json:"more"`
	CanDeleteWebDAV bool            `json:"canDeleteWebDAV"`
	WebDAVError     string          `json:"webdavError,omitempty"`
}

// RestoreChoices examines manifests in date order, one page at a time.
// A collection can exist before its manifest, or after a failed upload.
func RestoreChoices(ctx context.Context, offset int) (RestorePage, error) {
	var out RestorePage
	c, d, err := sessionBackupDAV()
	if err != nil {
		return out, err
	}
	ctx = withSessionDAVTimeouts(ctx)
	list, err := listSessionBackups(ctx, c, d)
	if err != nil {
		return out, err
	}
	out.Items = []RestoreChoice{}
	out.CanDeleteWebDAV, out.WebDAVError = list.CanDeleteWebDAV, list.WebDAVError
	start := min(max(offset, 0), len(list.Items))
	end := min(start+10, len(list.Items))
	out.More = end < len(list.Items)
	for _, entry := range list.Items[start:end] {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		row := inspectRestoreChoice(ctx, c, d, entry, true)
		out.Items = append(out.Items, row)
	}
	return out, nil
}

// RestoreDetail authenticates one manifest. A preview contains up to two
// titles; only a full inspection offers a backup for import.
func RestoreDetail(ctx context.Context, entry SessionBackup, verify bool) (RestoreChoice, error) {
	if !validBatchID(entry.ID) || (!entry.Local && !entry.WebDAV) {
		return RestoreChoice{}, errors.New("invalid session backup selection")
	}
	if len(entry.Layouts) > 3 {
		return RestoreChoice{}, errors.New("invalid session backup layout")
	}
	for _, layout := range entry.Layouts {
		if layout != sessionDAVDir && layout != batchDir && layout != legacyBatchDir {
			return RestoreChoice{}, errors.New("invalid session backup layout")
		}
	}
	c, d, err := sessionBackupDAV()
	if err != nil {
		return RestoreChoice{}, err
	}
	row := inspectRestoreChoice(withSessionDAVTimeouts(ctx), c, d, entry, verify)
	return row, ctx.Err()
}

func inspectRestoreChoice(ctx context.Context, c Config, d *dav, entry SessionBackup, verify bool) RestoreChoice {
	row := RestoreChoice{SessionBackup: entry}
	if entry.ID == currentSessionID {
		return currentStore(c, d).currentChoice(ctx, row, verify)
	}
	store := batchStore{dav: d, passphrase: c.Passphrase, batchID: entry.ID}
	inspect := func(raw []byte, err error, remote bool) bool {
		if err != nil {
			if !errors.Is(err, errSessionBackupMissing) && !errors.Is(err, fs.ErrNotExist) {
				row.Problem = err.Error()
			}
			return false
		}
		var m batchManifest
		version := 2
		if store.batchID == "" {
			version = 1
		}
		if err = store.open(raw, entry.ID, &m); err == nil && !m.valid(version) {
			err = errors.New("invalid session backup manifest")
		}
		if err == nil && verify {
			err = store.checkManifestFiles(ctx, m, remote)
		}
		if err != nil {
			row.Problem = err.Error()
			return false
		}
		row.Ready, row.RemoteReady, row.Device, row.Problem = verify, verify && remote, m.Device, ""
		for i, id := range m.Entries {
			title := ""
			if i < len(m.Info) {
				title = m.Info[i].Title
			}
			if !verify && title == "" {
				continue
			}
			row.Entries = append(row.Entries, restoreEntry{id, title})
			if !verify && len(row.Entries) == 2 {
				break
			}
		}
		return true
	}
	if entry.WebDAV {
		layouts := entry.Layouts
		if len(layouts) == 0 {
			layouts = []string{sessionDAVDir, batchDir, legacyBatchDir}
		}
		for _, layout := range layouts {
			if ctx.Err() != nil {
				return row
			}
			store.remoteDir, store.batchID = layout, entry.ID
			if layout == legacyBatchDir {
				store.batchID = ""
			}
			raw, err := store.remoteAt(ctx, "backups", entry.ID)
			if inspect(raw, err, true) {
				return row
			}
		}
	}
	if entry.Local && ctx.Err() == nil {
		store.batchID = entry.ID
		raw, err := readBatchFile(store.filename("backups", entry.ID))
		if errors.Is(err, fs.ErrNotExist) {
			store.batchID = ""
			raw, err = readBatchFile(store.filename("backups", entry.ID))
		}
		inspect(raw, err, false)
	}
	return row
}

func (s batchStore) checkManifestFiles(ctx context.Context, m batchManifest, remote bool) error {
	objects, err := m.objects()
	if err != nil {
		return err
	}
	var sizes map[string]remoteSessionFile
	if remote && s.batchID != "" {
		dir := s.remoteDir
		if dir == "" {
			dir = sessionDAVDir
		}
		collection := s.batchID
		if s.current {
			collection = currentSessionDir
		}
		dirs := []string{folder, dir, collection}
		if s.threadKey != "" {
			dirs = append(dirs, "threads", s.threadKey)
		}
		sizes, err = s.dav.listRemoteSizes(ctx, dirs...)
		if err != nil {
			return err
		}
	}
	for _, o := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !remote {
			info, err := os.Stat(s.filename(o.kind, o.id))
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() || info.Size() == 0 {
				return errors.New("missing session backup file")
			}
			continue
		}
		if sizes != nil {
			p := s.remoteObjectPath(o.kind, o.id)
			if n, ok := sizes[p[len(p)-1]]; !ok || n.size == 0 {
				return errors.New("missing session backup file")
			}
		} else {
			// A few DAV servers support GET but neither HEAD nor PROPFIND.
			// Validate the object itself if metadata cannot establish presence.
			_, exists, err := s.dav.remoteSize(ctx, s.url(o.kind, o.id))
			if err != nil {
				return err
			}
			if !exists {
				if err := s.verifyRemote(ctx, o.kind, o.id); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
