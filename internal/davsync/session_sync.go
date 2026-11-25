package davsync

import (
	"context"
	"errors"
	"time"
)

type sessionSyncState struct {
	At       time.Time `json:"at,omitzero"`
	Backup   string    `json:"backup,omitempty"`
	Uploaded int       `json:"uploaded,omitempty"`
	Skipped  int       `json:"skipped,omitempty"`
	Error    string    `json:"error,omitempty"`
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
	result, err := runSessionBatch(ctx, c, SessionBatchRequest{All: true})
	if errors.Is(err, errEmptySessionBatch) && !explicit {
		err = nil
	}
	if err != nil {
		status.Error = err.Error()
		return result, err
	}
	status.Error = ""
	status.At = time.Now()
	status.Backup = result.Backup
	status.Uploaded, status.Skipped = result.Uploaded, result.Skipped
	return result, nil
}
