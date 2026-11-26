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
		row := RestoreChoice{SessionBackup: entry}
		store := batchStore{dav: d, passphrase: c.Passphrase, batchID: entry.ID}
		var m batchManifest
		if entry.WebDAV {
			// Always inspect the remote manifest: a local copy can already
			// exist while the last remote PUT is still pending.
			raw, e := store.remote(ctx, "backups", entry.ID)
			version := 2
			if errors.Is(e, errSessionBackupMissing) {
				store.batchID = ""
				version = 1
				raw, e = store.remote(ctx, "backups", entry.ID)
			}
			if e == nil {
				e = store.open(raw, entry.ID, &m)
			}
			if e == nil && !m.valid(version) {
				e = errors.New("invalid session backup manifest")
			}
			if e == nil {
				e = store.checkManifestFiles(ctx, m, true)
			}
			if e == nil {
				row.Ready = true
				row.RemoteReady = true
			} else if !errors.Is(e, errSessionBackupMissing) {
				row.Problem = e.Error()
			}
		}
		if !row.Ready && entry.Local {
			store.batchID = entry.ID
			raw, e := readBatchFile(store.filename("backups", entry.ID))
			version := 2
			if errors.Is(e, fs.ErrNotExist) {
				store.batchID = ""
				version = 1
				raw, e = readBatchFile(store.filename("backups", entry.ID))
			}
			m = batchManifest{}
			if e == nil {
				e = store.open(raw, entry.ID, &m)
			}
			if e == nil && !m.valid(version) {
				e = errors.New("invalid session backup manifest")
			}
			if e == nil {
				e = store.checkManifestFiles(ctx, m, false)
			}
			if e == nil {
				row.Ready = true
			} else if e != nil {
				row.Problem = e.Error()
			}
		}
		if row.Ready {
			row.Device = m.Device
			for i, key := range m.Entries {
				title := ""
				if i < len(m.Info) {
					title = m.Info[i].Title
				}
				row.Entries = append(row.Entries, restoreEntry{key, title})
			}
		}
		out.Items = append(out.Items, row)
	}
	return out, nil
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
		sizes, err = s.dav.listRemoteSizes(ctx, folder, dir, s.batchID)
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
			if n, ok := sizes[s.remoteObjectPath(o.kind, o.id)[2]]; !ok || n.size == 0 {
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
