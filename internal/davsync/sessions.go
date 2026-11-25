package davsync

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/yetone/magpie/internal/backup"
	"github.com/yetone/magpie/internal/sessions"
)

const (
	codexSessionVersion = 1
	sessionArchiveLimit = 2*sessions.MaxCodexSnapshot + (4 << 20)
)

type codexSessionArchive struct {
	Version int                     `json:"version"`
	Agent   string                  `json:"agent"`
	ID      string                  `json:"id"`
	Files   []sessions.CodexRollout `json:"files"`
}

// SessionTransfer reports counts without exposing conversation text or paths.
type SessionTransfer struct {
	Changed int `json:"changed"`
}

// TransferCodexSession explicitly sends or receives one native session using
// the current WebDAV account. It is separate from both setup sync and its timer.
func TransferCodexSession(ctx context.Context, action, id string) (SessionTransfer, error) {
	id = strings.ToLower(strings.TrimSpace(id))
	if !sessions.ValidCodexSyncID(id) {
		return SessionTransfer{}, errors.New("enter a Codex session ID (UUID)")
	}
	if action != "push" && action != "pull" {
		return SessionTransfer{}, errors.New("choose push or pull for the Codex session")
	}
	c, ok := Load()
	if !ok || c.S3() || c.GitHub() {
		return SessionTransfer{}, errors.New("set up WebDAV sync before transferring Codex sessions")
	}
	if action == "push" && c.FollowServer {
		return SessionTransfer{}, errors.New("this computer follows the server and can only pull Codex sessions")
	}
	d, err := newDAV(c)
	if err != nil {
		return SessionTransfer{}, err
	}
	if action == "pull" {
		raw, _, err := d.readCodexSession(ctx, id)
		if err != nil {
			return SessionTransfer{}, err
		}
		if raw == nil {
			return SessionTransfer{}, errors.New("no copy of this Codex session on WebDAV")
		}
		files, err := openCodexSession(raw, c.Passphrase, id)
		if err != nil {
			return SessionTransfer{}, err
		}
		n, err := sessions.ImportCodexSnapshot(ctx, id, files)
		return SessionTransfer{Changed: n}, err
	}
	n, err := pushCodexSession(ctx, d, c.Passphrase, id)
	return SessionTransfer{Changed: n}, err
}

func pushCodexSession(ctx context.Context, d *dav, passphrase, id string) (int, error) {
	files, err := sessions.ReadCodexSnapshot(ctx, id)
	if err != nil {
		return 0, err
	}
	if len(files) == 0 {
		return 0, errors.New("no active Codex session with this ID in the current CODEX_HOME")
	}
	for _, dirs := range [][]string{{folder}, {folder, "sessions-v1"}, {folder, "sessions-v1", "codex"}} {
		if err := d.ensureSessionCollection(ctx, dirs); err != nil {
			return 0, err
		}
	}
	old, etag, err := d.readCodexSession(ctx, id)
	if err != nil {
		return 0, err
	}
	conditions := map[string]string{"If-None-Match": "*"}
	if old != nil {
		previous, err := openCodexSession(old, passphrase, id)
		if err != nil {
			return 0, err
		}
		if !sessions.CodexSnapshotExtends(files, previous) {
			return 0, sessions.ErrCodexSyncConflict
		}
		if sessions.CodexSnapshotExtends(previous, files) {
			return 0, nil
		}
		if len(etag) < 2 || etag[0] != '"' || etag[len(etag)-1] != '"' {
			return 0, errors.New("WebDAV did not give this session a strong ETag for a safe update")
		}
		conditions = map[string]string{"If-Match": etag}
	}
	raw, err := sealCodexSession(files, passphrase, id)
	if err != nil {
		return 0, err
	}
	res, err := d.do(ctx, http.MethodPut, d.codexSessionURL(id), raw, conditions)
	if err != nil {
		return 0, err
	}
	res.Body.Close()
	switch res.StatusCode {
	case http.StatusPreconditionFailed, http.StatusConflict:
		return 0, errors.New("another device updated the Codex session during transfer; try again")
	case http.StatusOK, http.StatusCreated, http.StatusNoContent:
	default:
		return 0, fmt.Errorf("writing the Codex session to WebDAV: HTTP %d", res.StatusCode)
	}
	written, _, err := d.readCodexSession(ctx, id)
	if err != nil {
		return 0, err
	}
	if !bytes.Equal(written, raw) {
		return 0, errors.New("WebDAV did not keep the full Codex session; try again")
	}
	return len(files), nil
}

func (d *dav) ensureSessionCollection(ctx context.Context, dirs []string) error {
	res, err := d.do(ctx, "MKCOL", d.url(dirs...)+"/", nil, nil)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode != http.StatusCreated && res.StatusCode != http.StatusMethodNotAllowed {
		return fmt.Errorf("making the Codex session folder on WebDAV: HTTP %d", res.StatusCode)
	}
	return nil
}

func (d *dav) codexSessionURL(id string) string {
	return d.url(folder, "sessions-v1", "codex", id+".magpie-session")
}

func (d *dav) readCodexSession(ctx context.Context, id string) ([]byte, string, error) {
	res, err := d.do(ctx, http.MethodGet, d.codexSessionURL(id), nil, nil)
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusNotFound, http.StatusGone, http.StatusConflict:
		return nil, "", nil
	case http.StatusOK:
	default:
		return nil, "", fmt.Errorf("reading the Codex session from WebDAV: HTTP %d", res.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, sessionArchiveLimit+1))
	if err != nil {
		return nil, "", err
	}
	if len(raw) > sessionArchiveLimit {
		return nil, "", errors.New("the WebDAV Codex session exceeds the sync limit")
	}
	return raw, res.Header.Get("ETag"), nil
}

func sealCodexSession(files []sessions.CodexRollout, passphrase, id string) ([]byte, error) {
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	err := json.NewEncoder(zw).Encode(codexSessionArchive{codexSessionVersion, "codex", id, files})
	if err := errors.Join(err, zw.Close()); err != nil {
		return nil, err
	}
	return backup.SealData(compressed.Bytes(), passphrase)
}

func openCodexSession(raw []byte, passphrase, id string) ([]sessions.CodexRollout, error) {
	compressed, err := backup.OpenData(raw, passphrase)
	if err != nil {
		return nil, err
	}
	zr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	plain, err := io.ReadAll(io.LimitReader(zr, sessionArchiveLimit+1))
	if err != nil {
		return nil, err
	}
	if len(plain) > sessionArchiveLimit {
		return nil, errors.New("the unpacked Codex session exceeds the sync limit")
	}
	var archive codexSessionArchive
	if json.Unmarshal(plain, &archive) != nil || archive.Version != codexSessionVersion ||
		archive.Agent != "codex" || archive.ID != id {
		return nil, errors.New("the WebDAV file does not contain this Codex session")
	}
	if err := sessions.ValidateCodexSnapshot(id, archive.Files); err != nil {
		return nil, err
	}
	return archive.Files, nil
}
