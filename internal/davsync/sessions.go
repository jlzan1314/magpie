package davsync

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yetone/magpie/internal/settings"
	"github.com/yetone/magpie/internal/steady"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
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
	Skipped int `json:"skipped,omitempty"`
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
	files, err := sessions.ReadCodexSnapshot(ctx, id)
	if err != nil {
		return SessionTransfer{}, err
	}
	if action == "push" && len(files) == 0 {
		return SessionTransfer{}, errors.New("no active Codex session with this ID in the current CODEX_HOME")
	}
	receiptPath := filepath.Join(settings.Dir(), "session-transfer-state", stateKey(c), id+".json")
	var receipt codexReceipt
	if saved, err := os.ReadFile(receiptPath); err == nil {
		_ = json.Unmarshal(saved, &receipt)
	}
	fingerprint := codexFingerprint(files)
	target := d.codexSessionURL(id)
	if receipt.Fingerprint == fingerprint && receipt.Tag != "" {
		head, err := d.send(ctx, http.MethodHead, target, nil, map[string]string{"Accept-Encoding": "identity"})
		if err != nil {
			return SessionTransfer{}, err
		}
		head.Body.Close()
		if head.StatusCode == http.StatusOK && head.Header.Get("ETag") == receipt.Tag {
			length := head.ContentLength
			if length <= 0 {
				if n, ok := d.length(ctx, target); ok {
					length = n
				}
			}
			if length == receipt.Size {
				return SessionTransfer{Skipped: 1}, nil
			}
		}
	}
	old, tag, err := d.readCodexSession(ctx, id)
	if err != nil {
		return SessionTransfer{}, err
	}
	var previous []sessions.CodexRollout
	if old != nil {
		previous, err = openCodexSession(old, c.Passphrase, id)
		if err != nil {
			return SessionTransfer{}, err
		}
	}
	if action == "pull" {
		if old == nil {
			return SessionTransfer{}, errors.New("no copy of this Codex session on WebDAV")
		}
		count, err := sessions.ImportCodexSnapshot(ctx, id, previous)
		if err != nil {
			return SessionTransfer{}, err
		}
		rememberCodexReceipt(receiptPath, codexFingerprint(previous), tag, len(old))
		return SessionTransfer{Changed: count}, nil
	}
	headers := map[string]string{"If-None-Match": "*"}
	if old != nil {
		if !sessions.CodexSnapshotExtends(files, previous) {
			return SessionTransfer{}, sessions.ErrCodexSyncConflict
		}
		if sessions.CodexSnapshotExtends(previous, files) {
			rememberCodexReceipt(receiptPath, fingerprint, tag, len(old))
			return SessionTransfer{}, nil
		}
		if len(tag) < 2 || tag[0] != '"' || tag[len(tag)-1] != '"' {
			return SessionTransfer{}, errors.New("WebDAV did not give this session a strong ETag for a safe update")
		}
		headers = map[string]string{"If-Match": tag}
	} else {
		for _, dirs := range [][]string{{folder}, {folder, "sessions-v1"}, {folder, "sessions-v1", "codex"}} {
			if err := d.ensureSessionCollection(ctx, dirs); err != nil {
				return SessionTransfer{}, err
			}
		}
	}
	raw, err := sealCodexSession(files, c.Passphrase, id)
	if err != nil {
		return SessionTransfer{}, err
	}
	res, err := d.do(ctx, http.MethodPut, target, raw, headers)
	if err != nil {
		return SessionTransfer{}, err
	}
	res.Body.Close()
	switch res.StatusCode {
	case http.StatusCreated, http.StatusOK, http.StatusNoContent:
	case http.StatusConflict, http.StatusPreconditionFailed:
		return SessionTransfer{}, errors.New("another device updated the Codex session during transfer; try again")
	default:
		return SessionTransfer{}, fmt.Errorf("writing the Codex session to WebDAV: HTTP %d", res.StatusCode)
	}
	got, tag, err := d.readCodexSession(ctx, id)
	if err != nil {
		return SessionTransfer{}, err
	}
	if !bytes.Equal(got, raw) {
		return SessionTransfer{}, errors.New("WebDAV did not keep the full Codex session; try again")
	}
	rememberCodexReceipt(receiptPath, fingerprint, tag, len(raw))
	return SessionTransfer{Changed: len(files)}, nil
}

type codexReceipt struct {
	Fingerprint string
	Tag         string
	Size        int64
}

func codexFingerprint(files []sessions.CodexRollout) string {
	hash := sha256.New()
	_ = json.NewEncoder(hash).Encode(files)
	return hex.EncodeToString(hash.Sum(nil))
}

func rememberCodexReceipt(filename, fingerprint, tag string, size int) {
	if len(tag) < 2 || tag[0] != '"' || tag[len(tag)-1] != '"' {
		return
	}
	data, err := json.Marshal(codexReceipt{fingerprint, tag, int64(size)})
	if err == nil && os.MkdirAll(filepath.Dir(filename), 0o700) == nil {
		_ = steady.WriteFile(filename, data, 0o600)
	}
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
	res, err := d.do(ctx, http.MethodGet, d.codexSessionURL(id), nil, map[string]string{"Accept-Encoding": "identity"})
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
	ordered := slices.Clone(files)
	slices.SortFunc(ordered, func(a, b sessions.CodexRollout) int {
		return strings.Compare(a.Path, b.Path)
	})
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	err := json.NewEncoder(zw).Encode(codexSessionArchive{codexSessionVersion, "codex", id, ordered})
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
