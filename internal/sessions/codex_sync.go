package sessions

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/tidwall/gjson"

	"github.com/yetone/magpie/internal/filelock"
	"github.com/yetone/magpie/internal/steady"
)

// MaxCodexSnapshot is the maximum total size of a session's unpacked rollouts.
const MaxCodexSnapshot = 64 << 20

// CodexRollout is a native, unpacked rollout under CODEX_HOME/sessions.
// A session with multiple rollouts keeps them together.
type CodexRollout struct {
	Path string `json:"path"`
	Data []byte `json:"data"`
}

var ErrCodexSyncConflict = errors.New("the Codex session has different history on the two devices")

func ValidCodexSyncID(id string) bool {
	return len(id) == 36 && codexThreadOf("rollout-2000-01-01T00-00-00-"+id+".jsonl") == id
}

func validCodexSyncPath(id, name string) bool {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || len(parts[0]) != 4 || len(parts[1]) != 2 || len(parts[2]) != 2 ||
		!strings.HasSuffix(parts[3], ".jsonl") || codexThreadOf(parts[3]) != id {
		return false
	}
	for _, part := range parts[:3] {
		for _, c := range part {
			if c < '0' || c > '9' {
				return false
			}
		}
	}
	return true
}

func ValidateCodexSnapshot(id string, files []CodexRollout) error {
	if !ValidCodexSyncID(id) || len(files) == 0 {
		return errors.New("invalid Codex session snapshot")
	}
	var total int
	seen := make(map[string]bool, len(files))
	for _, f := range files {
		if !validCodexSyncPath(id, f.Path) || seen[f.Path] {
			return errors.New("invalid Codex rollout path in the snapshot")
		}
		seen[f.Path] = true
		if len(f.Data) == 0 || len(f.Data) > MaxCodexSnapshot-total {
			return errors.New("the Codex session exceeds the 64 MiB sync limit")
		}
		total += len(f.Data)
		first, _, complete := bytes.Cut(f.Data, []byte("\n"))
		if !complete || f.Data[len(f.Data)-1] != '\n' || !gjson.ValidBytes(first) ||
			gjson.GetBytes(first, "type").String() != "session_meta" ||
			strings.ToLower(gjson.GetBytes(first, "payload.id").String()) != id {
			return errors.New("the snapshot contains an incomplete or unrelated Codex rollout")
		}
	}
	return nil
}

// CodexSnapshotExtends rejects concurrent histories and binary rewrites.
// The source must contain every rollout and byte already at the destination.
func CodexSnapshotExtends(source, dest []CodexRollout) bool {
	byPath := make(map[string][]byte, len(source))
	for _, f := range source {
		byPath[f.Path] = f.Data
	}
	for _, f := range dest {
		if !bytes.HasPrefix(byPath[f.Path], f.Data) {
			return false
		}
	}
	return true
}

// ReadCodexSnapshot takes a stable copy without following any rollout symlinks.
func ReadCodexSnapshot(ctx context.Context, id string) ([]CodexRollout, error) {
	if !ValidCodexSyncID(id) {
		return nil, errors.New("enter a Codex session ID (UUID)")
	}
	files, _, err := readCodexSnapshot(ctx, filepath.Join(CodexDir(), "sessions"), id)
	if err == nil && len(files) != 0 {
		err = ValidateCodexSnapshot(id, files)
	}
	return files, err
}

func readCodexSnapshot(ctx context.Context, folder, id string) ([]CodexRollout, map[string]string, error) {
	root, err := os.OpenRoot(folder)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	defer root.Close()
	var files []CodexRollout
	paths := make(map[string]string)
	left := int64(MaxCodexSnapshot)
	err = fs.WalkDir(root.FS(), ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		logical := strings.TrimSuffix(name, ".zst")
		if !d.Type().IsRegular() || !validCodexSyncPath(id, logical) {
			return nil
		}
		data, err := readSyncRollout(root, name, left)
		if err != nil {
			return err
		}
		if old, found := paths[logical]; found {
			return fmt.Errorf("both %s and %s hold the same Codex rollout", path.Base(old), path.Base(name))
		}
		left -= int64(len(data))
		files = append(files, CodexRollout{logical, data})
		paths[logical] = name
		return nil
	})
	return files, paths, err
}

func readSyncRollout(root *os.Root, name string, limit int64) ([]byte, error) {
	f, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("Codex rollout is not a regular file")
	}
	var r io.Reader = f
	if strings.HasSuffix(name, ".zst") {
		z, err := zstd.NewReader(f, zstd.WithDecoderMaxMemory(MaxCodexSnapshot))
		if err != nil {
			return nil, err
		}
		defer z.Close()
		r = z
	}
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("the Codex session exceeds the 64 MiB sync limit")
	}
	after, err := root.Stat(name)
	if err != nil || !os.SameFile(before, after) || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		return nil, errRolloutHeld
	}
	return data, nil
}

// ImportCodexSnapshot fast-forwards the native files under Codex's writer
// lock. Each rollout is replaced atomically and a retry accepts identical
// segments if an earlier import was interrupted.
func ImportCodexSnapshot(ctx context.Context, id string, files []CodexRollout) (int, error) {
	if err := ValidateCodexSnapshot(id, files); err != nil {
		return 0, err
	}
	home := CodexDir()
	folder := filepath.Join(home, "sessions")
	locks := filepath.Join(home, "thread-writer-locks")
	if err := os.MkdirAll(locks, 0o700); err != nil {
		return 0, err
	}
	unlock, held := filelock.Lock(filepath.Join(locks, ".coordination.lock"), filelock.Wait)
	if !held {
		return 0, errRolloutHeld
	}
	defer unlock()
	lf, err := os.OpenFile(filepath.Join(locks, id+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, err
	}
	defer lf.Close()
	if ok, err := filelock.TryLock(lf); err != nil || !ok {
		return 0, errRolloutHeld
	}
	defer filelock.Unlock(lf)
	local, paths, err := readCodexSnapshot(ctx, folder, id)
	if err != nil {
		return 0, err
	}
	if !CodexSnapshotExtends(files, local) {
		return 0, ErrCodexSyncConflict
	}
	if err := os.MkdirAll(folder, 0o700); err != nil {
		return 0, err
	}
	root, err := os.OpenRoot(folder)
	if err != nil {
		return 0, err
	}
	defer root.Close()
	prev := make(map[string][]byte, len(local))
	for _, f := range local {
		prev[f.Path] = f.Data
	}
	var written int
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		if bytes.Equal(prev[f.Path], f.Data) {
			continue
		}
		target := paths[f.Path]
		if err := writeSyncRollout(root, f, target); err != nil {
			return written, err
		}
		written++
	}
	forgetWarmListing()
	return written, nil
}

func writeSyncRollout(root *os.Root, f CodexRollout, existing string) error {
	dest := f.Path
	if existing != "" {
		dest = existing
	}
	dir := path.Dir(dest)
	if err := root.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp := path.Join(dir, ".magpie-session-"+rand.Text())
	out, err := root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer root.Remove(tmp)
	var writeErr error
	if strings.HasSuffix(dest, ".zst") {
		z, err := zstd.NewWriter(out)
		if err != nil {
			out.Close()
			return err
		}
		_, writeErr = z.Write(f.Data)
		writeErr = errors.Join(writeErr, z.Close())
	} else {
		_, writeErr = out.Write(f.Data)
	}
	if writeErr == nil {
		writeErr = steady.Sync(out)
	}
	if err := errors.Join(writeErr, out.Close()); err != nil {
		return err
	}
	if existing == "" {
		return root.Link(tmp, dest)
	}
	return root.Rename(tmp, dest)
}
