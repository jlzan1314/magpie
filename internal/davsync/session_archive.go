package davsync

import (
	"archive/zip"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/yetone/magpie/internal/backup"
	"github.com/yetone/magpie/internal/sessions"
)

func init() { backup.CaptureSessions = writeSessionArchive }

func writeSessionArchive(ctx context.Context, w io.Writer, pass string) (string, error) {
	entries, err := sessions.CollectBatch(ctx, nil, true)
	if err != nil || len(entries) == 0 {
		return "", err
	}
	dir, err := os.MkdirTemp("", "magpie-local-sessions-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	store := batchStore{localRoot: dir, passphrase: pass}
	id, _, err := store.save(ctx, sessions.MergeBatch(entries), false, batchPartSize)
	if err != nil {
		return "", err
	}
	z := zip.NewWriter(w)
	// Objects are already compressed and sealed.
	err = filepath.WalkDir(dir, func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(dir, name)
		if err != nil {
			return err
		}
		dst, err := z.CreateHeader(&zip.FileHeader{Name: filepath.ToSlash(relative), Method: zip.Store})
		if err != nil {
			return err
		}
		src, err := os.Open(name)
		if err != nil {
			return err
		}
		defer src.Close()
		_, err = io.Copy(dst, src)
		return err
	})
	return id, errors.Join(err, z.Close())
}

func RestoreAutoSessions(ctx context.Context, name, pass, cwd string, targets []string) ([]sessions.BatchImport, error) {
	archive, id, configured, err := backup.OpenAutoSessions(name)
	if err != nil {
		return nil, err
	}
	defer archive.Close()
	if pass == "" {
		pass = configured
	}
	store := batchStore{passphrase: pass, archive: archive}
	entries, err := store.load(ctx, id)
	if err != nil {
		return nil, err
	}
	return sessions.ImportBatch(ctx, entries, targets, cwd)
}
