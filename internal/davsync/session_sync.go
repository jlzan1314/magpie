package davsync

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"time"
)

type sessionSyncState struct {
	At       time.Time `json:"at,omitzero"`
	Backup   string    `json:"backup,omitempty"`
	Uploaded int       `json:"uploaded,omitempty"`
	Skipped  int       `json:"skipped,omitempty"`
	Error    string    `json:"error,omitempty"`
	Pending  string    `json:"pending,omitempty"`
}

const sessionUploadInterrupted = "Session backup upload was interrupted. The next sync will resume it."

func (s sessionSyncState) interrupted() bool {
	return s.Error == context.Canceled.Error() || s.Error == context.DeadlineExceeded.Error() ||
		strings.HasSuffix(s.Error, ": "+context.Canceled.Error()) ||
		strings.HasSuffix(s.Error, ": "+context.DeadlineExceeded.Error())
}

func (s sessionSyncState) pendingBackup() (id, remoteDir string) {
	// Versions without Pending kept the failing object's URL. It also
	// identifies the collection where an upload had already started.
	if s.interrupted() {
		for _, dir := range []string{sessionDAVDir, batchDir} {
			prefix := "/" + dir + "/"
			i := strings.LastIndex(s.Error, prefix)
			if i < 0 {
				continue
			}
			id, _, _ := strings.Cut(s.Error[i+len(prefix):], "/")
			if validBatchID(id) && (s.Pending == "" || id == s.Pending) {
				return id, dir
			}
		}
	}
	return s.Pending, ""
}

func (s sessionSyncState) currentError() string {
	// These diagnostics were saved before session size was checked on the
	// packed object. They do not describe a failure the current code can make.
	if strings.HasSuffix(s.Error, ": session exceeds the 16 MiB portable transcript limit") ||
		strings.HasSuffix(s.Error, ": session exceeds the 64 MiB uncompressed transcript limit") {
		return ""
	}
	if s.interrupted() {
		if id, _ := s.pendingBackup(); id != "" {
			return sessionUploadInterrupted
		}
	}
	return s.Error
}

// SyncLocalSessions incrementally syncs all locally changed readable sessions
// to WebDAV, skipping packed objects and manifests whose sizes match remote.
func SyncLocalSessions(ctx context.Context) (SessionBatchResult, error) {
	lctx, lcancel := context.WithTimeout(ctx, wait)
	unlock, err := lock(lctx)
	lcancel()
	if err != nil {
		return SessionBatchResult{}, err
	}
	defer unlock()
	mu.Lock()
	defer mu.Unlock()

	c, ok := Load()
	if !ok || c.S3() || c.GitHub() {
		return SessionBatchResult{}, errors.New("set up WebDAV sync before backing up sessions")
	}
	if c.FollowServer {
		return SessionBatchResult{}, errors.New("this computer follows the server and cannot upload local session changes")
	}
	st := loadState()
	if st.Key != stateKey(c) {
		st = state{Key: stateKey(c), Brought: st.Brought, Replaced: st.Replaced, Own: st.Own, Follow: st.Follow, Here: st.Here}
	}
	res, err := recordSessionSync(ctx, c, &st, true)
	saveState(st)
	return res, err
}

func shareSessions(ctx context.Context, c Config, st *state) {
	if !c.Sessions || c.FollowServer || c.S3() || c.GitHub() {
		st.Sessions = nil
		return
	}
	_, _ = recordSessionSync(ctx, c, st, false)
}

func recordSessionSync(ctx context.Context, c Config, st *state, explicit bool) (SessionBatchResult, error) {
	if st.Sessions == nil {
		st.Sessions = &sessionSyncState{}
	}
	status := st.Sessions
	var result SessionBatchResult
	var err error
	pending, remoteDir := status.pendingBackup()
	status.Pending = pending
	if status.Pending != "" {
		result, err = resumeSessionBatch(ctx, c, status.Pending, remoteDir)
		if errors.Is(err, fs.ErrNotExist) {
			status.Pending = ""
		}
	}
	if status.Pending == "" {
		result, err = runSessionBatch(ctx, c, SessionBatchRequest{All: true}, func(id string) {
			status.Pending = id
			saveState(*st)
		})
	}
	if errors.Is(err, errEmptySessionBatch) && !explicit {
		err = nil
	}
	if err != nil {
		status.Error = err.Error()
		return result, err
	}
	status.Error = ""
	status.Pending = ""
	status.At = time.Now()
	status.Backup = result.Backup
	status.Uploaded, status.Skipped = result.Uploaded, result.Skipped
	return result, nil
}
