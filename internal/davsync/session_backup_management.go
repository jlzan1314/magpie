package davsync

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/yetone/magpie/internal/settings"
)

type SessionBackup struct {
	remoteTime time.Time // remote collection timestamp, independent of a later local restore
	ID         string    `json:"id"`
	Local      bool      `json:"local"`
	WebDAV     bool      `json:"webdav"`
	Updated    time.Time `json:"updated,omitzero"`
}

type SessionBackups struct {
	Items           []SessionBackup `json:"items"`
	CanDeleteWebDAV bool            `json:"canDeleteWebDAV"`
	WebDAVError     string          `json:"webdavError,omitempty"`
}

func ListSessionBackups(ctx context.Context) (SessionBackups, error) {
	c, d, err := sessionBackupDAV()
	if err != nil {
		return SessionBackups{}, err
	}
	return listSessionBackups(ctx, c, d)
}

func listSessionBackups(ctx context.Context, c Config, d *dav) (SessionBackups, error) {
	out := SessionBackups{Items: []SessionBackup{}, CanDeleteWebDAV: !c.FollowServer}
	items := map[string]SessionBackup{}
	for _, v := range []string{batchDir, legacyBatchDir} {
		parts := []string{v}
		if v == legacyBatchDir {
			parts = append(parts, "backups")
		}
		root := filepath.Join(append([]string{settings.Dir()}, parts...)...)
		entries, err := os.ReadDir(root)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return out, err
		}
		for _, e := range entries {
			id := backupEntryID(e.Name(), v)
			if id == "" {
				continue
			}
			p := filepath.Join(root, e.Name())
			if v == batchDir {
				p = filepath.Join(p, "manifest.sealed")
			}
			if fi, err := os.Stat(p); err == nil && fi.Mode().IsRegular() {
				i := items[id]
				i.ID, i.Local, i.Updated = id, true, fi.ModTime()
				items[id] = i
			}
		}
	}
	for _, parts := range [][]string{{sessionDAVDir}, {batchDir}, {legacyBatchDir, "backups"}} {
		remote, err := listWebDAVSessionBackups(ctx, d, parts)
		if err != nil {
			out.WebDAVError = err.Error()
			continue
		}
		for id, modified := range remote {
			i := items[id]
			i.ID, i.WebDAV = id, true
			if modified.After(i.remoteTime) {
				i.remoteTime = modified
			}
			if modified.After(i.Updated) {
				i.Updated = modified
			}
			items[id] = i
		}
	}
	for _, i := range items {
		out.Items = append(out.Items, i)
	}
	slices.SortFunc(out.Items, func(a, b SessionBackup) int {
		if n := b.Updated.Compare(a.Updated); n != 0 {
			return n
		}
		return strings.Compare(a.ID, b.ID)
	})
	return out, nil
}

func backupEntryID(name, v string) string {
	if v == legacyBatchDir {
		var ok bool
		name, ok = strings.CutSuffix(name, ".sealed")
		if !ok {
			return ""
		}
	}
	if validBatchID(name) {
		return name
	}
	return ""
}

func listWebDAVSessionBackups(ctx context.Context, d *dav, parts []string) (map[string]time.Time, error) {
	address := d.url(append([]string{folder}, parts...)...) + "/"
	res, err := d.send(ctx, "PROPFIND", address, nil, map[string]string{"Depth": "1"})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusNotFound, http.StatusGone, http.StatusConflict:
		return nil, nil
	case http.StatusMultiStatus:
	default:
		return nil, fmt.Errorf("listing session backups on WebDAV: HTTP %d", res.StatusCode)
	}
	var multistatus struct {
		Entries []struct {
			Href     string `xml:"href"`
			Modified string `xml:"propstat>prop>getlastmodified"`
		} `xml:"response"`
	}
	if err := xml.NewDecoder(io.LimitReader(res.Body, 4<<20)).Decode(&multistatus); err != nil {
		return nil, err
	}
	base, _ := url.Parse(address)
	out := map[string]time.Time{}
	for _, e := range multistatus.Entries {
		u, err := base.Parse(e.Href)
		if err != nil || u.Host != base.Host {
			continue
		}
		p := pathpkg.Clean(u.Path)
		if pathpkg.Dir(p) != pathpkg.Clean(base.Path) {
			continue
		}
		if id := backupEntryID(pathpkg.Base(p), parts[0]); id != "" {
			out[id], _ = http.ParseTime(e.Modified)
		}
	}
	return out, nil
}

func sessionBackupDAV() (Config, *dav, error) {
	c, ok := Load()
	if !ok || c.S3() || c.GitHub() {
		return Config{}, nil, errors.New("set up WebDAV sync before managing session backups")
	}
	d, err := newDAV(c)
	return c, d, err
}

// DeleteSessionBackup removes only an explicitly named backup from the
// chosen place. Native Agent sessions and other backups are independent.
func DeleteSessionBackup(ctx context.Context, id, scope string) error {
	id = strings.TrimSpace(id)
	if !validBatchID(id) {
		return errors.New("enter a valid session backup ID")
	}
	c, d, err := sessionBackupDAV()
	if err != nil {
		return err
	}
	switch scope {
	case "local":
		if err := removeLocalSessionBackup(batchDir, id, false); err != nil {
			return err
		}
		return removeLocalSessionBackup(filepath.Join(legacyBatchDir, "backups"), id+".sealed", true)
	case "webdav":
		if c.FollowServer {
			return errors.New("this computer follows the server and can only delete local session backups")
		}
		for _, dir := range []string{sessionDAVDir, batchDir} {
			if _, err := deleteDAVBackup(ctx, d, d.url(folder, dir, id)+"/"); err != nil {
				return err
			}
		}
		_, err := deleteDAVBackup(ctx, d, d.url(folder, legacyBatchDir, "backups", id+".sealed"))
		if err != nil {
			return err
		}
		return locked(func() error {
			hint := batchHint(c)
			if raw, err := os.ReadFile(hint); err == nil && string(raw) == id {
				if err := writeBatchFile(hint, nil); err != nil {
					return err
				}
			}
			st := loadState()
			if st.Key == stateKey(c) && st.Sessions != nil && st.Sessions.Backup == id {
				st.Sessions.Backup = ""
				st.Sessions.At = time.Time{}
				return saveState(st)
			}
			return nil
		})
	default:
		return errors.New("choose local or WebDAV session backup")
	}
}

func removeLocalSessionBackup(dir, name string, manifestOnly bool) error {
	root, err := os.OpenRoot(filepath.Join(settings.Dir(), dir))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer root.Close()
	if manifestOnly {
		err := root.Remove(name)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	return root.RemoveAll(name)
}

func deleteDAVBackup(ctx context.Context, d *dav, address string) (bool, error) {
	res, err := d.send(ctx, http.MethodDelete, address, nil, nil)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		return true, nil
	case http.StatusNotFound, http.StatusGone, http.StatusConflict:
		return false, nil
	default:
		return false, fmt.Errorf("deleting session backup from WebDAV: HTTP %d", res.StatusCode)
	}
}
