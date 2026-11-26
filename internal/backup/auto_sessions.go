package backup

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/yetone/magpie/internal/filelock"
)

// CaptureSessions writes a portable archive of sealed session objects and returns
// its content ID. The application supplies the same codec used by WebDAV.
var CaptureSessions func(context.Context, io.Writer, string, string) (string, error)

func sessionArchivePath(dir, name string) string {
	return filepath.Join(dir, "session_"+strings.TrimSuffix(name, ".json")+".zip")
}

func autoContents(ctx context.Context, c AutoConfig) (autoSealed, error) {
	sealed, err := autoSignIns(c)
	if err != nil || !c.Sessions {
		return sealed, err
	}
	if CaptureSessions == nil || c.Passphrase == "" {
		return sealed, errors.New("session backup needs an archive writer and a passphrase")
	}
	if err = os.MkdirAll(c.dir(), 0700); err != nil {
		return sealed, err
	}
	f, err := os.CreateTemp(c.dir(), autoTemp)
	if err != nil {
		return sealed, err
	}
	sealed.sessionTemp = f.Name()
	hash := sha256.New()
	sealed.sessionID, err = CaptureSessions(ctx, io.MultiWriter(f, hash), c.Passphrase, c.dir())
	sealed.sessionSHA = hex.EncodeToString(hash.Sum(nil))
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil || sealed.sessionID == "" {
		sealed.discard()
		sealed.sessionTemp = ""
		sealed.sessionSHA = ""
	}
	return sealed, err
}

func (s autoSealed) discard() {
	if s.sessionTemp != "" {
		os.Remove(s.sessionTemp)
	}
}

// The old version can be reused only if its companion is present and opens
// with today's passphrase (which may have changed since the last backup).
func previousArchiveUsable(dir string, b AutoBackup, pass string) bool {
	if !b.Sessions {
		return true
	}
	file, err := os.Open(sessionArchivePath(dir, b.Name))
	if err != nil {
		return false
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil || hex.EncodeToString(hash.Sum(nil)) != b.sessionSHA {
		return false
	}
	z, err := zip.NewReader(file, size)
	if err != nil {
		return false
	}
	f, err := z.Open("session-batches-v2/" + b.sessionID + "/manifest.sealed")
	if err != nil {
		return false
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 64<<20))
	if err != nil {
		return false
	}
	_, err = OpenData(data, pass)
	return err == nil
}

// OpenAutoSessions opens only the companion of a validated setup backup.
func OpenAutoSessions(name string) (*AutoSessionFiles, string, string, error) {
	if !autoName.MatchString(name) {
		return nil, "", "", errors.New("not a backup's name")
	}
	c, err := LoadAuto()
	if err != nil {
		return nil, "", "", err
	}
	b, _, err := readAuto(filepath.Join(c.dir(), name))
	if err != nil {
		return nil, "", "", err
	}
	if !b.Sessions {
		return nil, "", "", errors.New("this backup has no sessions")
	}
	unlock, err := lockAutoPool(c)
	if err != nil {
		return nil, "", "", err
	}
	z, err := zip.OpenReader(sessionArchivePath(c.dir(), name))
	if err != nil {
		unlock()
		return nil, "", "", err
	}
	return &AutoSessionFiles{ReadCloser: z, Pool: filepath.Join(c.dir(), SessionPool), unlock: unlock}, b.sessionID, c.Passphrase, nil
}

const SessionPool = "session_objects"

type AutoSessionFiles struct {
	*zip.ReadCloser
	Pool   string
	unlock func()
}

func (f *AutoSessionFiles) Close() error { defer f.unlock(); return f.ReadCloser.Close() }

// Serializes publication and garbage collection across the gateway and CLI.
func lockAutoPool(c AutoConfig) (func(), error) {
	pool := filepath.Join(c.dir(), SessionPool)
	st, err := os.Lstat(pool)
	if errors.Is(err, fs.ErrNotExist) && !c.Sessions {
		return func() {}, nil
	}
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if err == nil && !st.IsDir() {
		return nil, errors.New("session_objects must be a directory in the backup folder")
	}
	unlock, ok := filelock.Lock(filepath.Join(c.dir(), ".session-objects.lock"), filelock.Wait)
	if !ok {
		return nil, errors.New("another process is using the local session backups")
	}
	return unlock, nil
}

func pruneSessionPool(dir string) {
	pool := filepath.Join(dir, SessionPool)
	objects, err := os.ReadDir(pool)
	if err != nil {
		return
	}
	all, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	kept := map[string]bool{}
	for _, f := range all {
		name := f.Name()
		if !strings.HasPrefix(name, "session_") || !strings.HasSuffix(name, ".zip") {
			continue
		}
		backupName := strings.TrimSuffix(strings.TrimPrefix(name, "session_"), ".zip") + ".json"
		if !autoName.MatchString(backupName) {
			continue
		}
		archive, err := zip.OpenReader(filepath.Join(dir, name))
		if err != nil {
			return
		} // an unreadable version may still own objects
		for _, member := range archive.File {
			if id, ok := strings.CutPrefix(member.Name, SessionPool+"/"); ok {
				kept[id] = true
			}
		}
		archive.Close()
	}
	for _, f := range objects {
		id, ok := strings.CutSuffix(f.Name(), ".sealed")
		if !ok || len(id) != 64 || !f.Type().IsRegular() || kept[f.Name()] {
			continue
		}
		if _, e := hex.DecodeString(id); e == nil {
			os.Remove(filepath.Join(pool, f.Name()))
		}
	}
}
