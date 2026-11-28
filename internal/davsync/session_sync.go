package davsync

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"time"

	"github.com/yetone/magpie/internal/sessions"
)

type sessionSyncState struct {
	Imported      int                     `json:"imported,omitempty"`
	ImportError   string                  `json:"importError,omitempty"`
	ImportedAt    time.Time               `json:"importedAt,omitzero"`
	Conversations int                     `json:"conversations,omitempty"`
	Request       SessionRetry            `json:"request,omitzero"`
	Failed        []sessions.BatchFailure `json:"failed,omitempty"`
	Explicit      bool                    `json:"explicit,omitempty"`
	At            time.Time               `json:"at,omitzero"`
	Backup        string                  `json:"backup,omitempty"`
	Uploaded      int                     `json:"uploaded,omitempty"`
	Skipped       int                     `json:"skipped,omitempty"`
	Error         string                  `json:"error,omitempty"`
	Pending       string                  `json:"pending,omitempty"`
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

func sessionImportDiagnostic(message string) string {
	canceled := context.Canceled.Error()
	if !strings.Contains(message, canceled) {
		return message
	}
	var failures []string
	for line := range strings.SplitSeq(message, "\n") {
		if line != canceled && !strings.HasSuffix(line, ": "+canceled) {
			failures = append(failures, line)
		}
	}
	return strings.Join(failures, "\n")
}

func sessionUploadConfig() (Config, error) {
	c, ok := Load()
	if !ok || c.S3() || c.GitHub() {
		return c, errors.New("set up WebDAV sync before backing up sessions")
	}
	if c.FollowServer {
		return c, errors.New("this computer follows the server and cannot upload local session changes")
	}
	return c, nil
}

// SyncLocalSessions incrementally syncs all locally changed readable sessions
// to WebDAV, skipping packed objects and manifests whose sizes match remote.
func SyncLocalSessions(ctx context.Context) (SessionBatchResult, error) {
	c, err := sessionUploadConfig()
	if err != nil {
		return SessionBatchResult{}, err
	}
	job, owner := beginSessionJob(c)
	if !owner {
		select {
		case <-ctx.Done():
			return SessionBatchResult{}, ctx.Err()
		case <-job.done:
			return job.result, job.err
		}
	}
	result, err := syncLocalSessionJob(ctx, c, job, SessionRetry{})
	job.finish(result, err)
	return result, err
}

func syncLocalSessionJob(ctx context.Context, requested Config, job *sessionJob, retry SessionRetry) (SessionBatchResult, error) {
	ctx = context.WithValue(ctx, sessionJobKey{}, job)
	lctx, lcancel := context.WithTimeout(ctx, wait)
	unlock, err := lock(lctx)
	lcancel()
	if err != nil {
		return SessionBatchResult{}, err
	}
	defer unlock()
	mu.Lock()
	defer mu.Unlock()

	c, err := sessionUploadConfig()
	if err != nil {
		return SessionBatchResult{}, err
	}
	if stateKey(c) != stateKey(requested) {
		return SessionBatchResult{}, errors.New("WebDAV settings changed before the upload started")
	}
	st := loadState()
	if st.Key != stateKey(c) {
		st = state{Key: stateKey(c), Brought: st.Brought, Replaced: st.Replaced, Own: st.Own, Follow: st.Follow, Here: st.Here}
	}
	res, err := recordSessionSync(ctx, c, &st, true, retry)
	if writeErr := saveState(st); writeErr != nil {
		return res, errors.Join(err, writeErr)
	}
	return res, err
}

func shareSessions(ctx context.Context, c Config, st *state) {
	if c.S3() || c.GitHub() {
		return
	}
	upload := c.Sessions && !c.FollowServer
	prefs, prefErr := LoadSessionPreferences()
	if !upload && !prefs.AutoImport && prefErr == nil {
		return
	}
	job, owner := beginSessionJob(c)
	if !owner {
		return
	}
	ctx = context.WithValue(ctx, sessionJobKey{}, job)
	var result SessionBatchResult
	var uploadErr error
	if upload {
		result, uploadErr = recordSessionSync(ctx, c, st, false, SessionRetry{})
	}
	if st.Sessions == nil {
		st.Sessions = &sessionSyncState{}
	}
	var importError string
	switch {
	case prefErr != nil:
		importError = prefErr.Error()
	case prefs.AutoImport && ctx.Err() != nil:
		importError = st.Sessions.ImportError
	case prefs.AutoImport:
		updateSessionActivity(ctx, func(a *SessionActivity) { a.Phase = "importing" })
		n, err := receiveSessionBackups(ctx, c, prefs)
		if err != nil {
			importError = err.Error()
		}
		if n > 0 {
			st.Sessions.Imported = n
			st.Sessions.ImportedAt = time.Now()
		}
	}
	st.Sessions.ImportError = sessionImportDiagnostic(importError)
	job.finish(result, uploadErr)
}

func recordSessionSync(ctx context.Context, c Config, st *state, explicit bool, retry SessionRetry) (SessionBatchResult, error) {
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
		result.Failed = append(result.Failed, status.Failed...)
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, errChanged) {
			status.Pending = ""
		}
	}
	if status.Pending == "" {
		failedOnly := retry.FailedOnly || retry.Source != nil
		if status.Explicit && !failedOnly {
			retry = status.Request
			failedOnly = retry.FailedOnly || retry.Source != nil
		}
		req := SessionBatchRequest{All: !failedOnly}
		var outstanding []sessions.BatchFailure
		if failedOnly {
			for _, failure := range status.Failed {
				if retry.Source != nil && *retry.Source != failure.Source {
					outstanding = append(outstanding, failure)
					continue
				}
				req.Sources = append(req.Sources, failure.Source)
			}
			if status.Backup != "" && status.Backup != currentSessionID {
				req.Backups = []string{status.Backup}
			}
			if len(req.Sources) == 0 {
				return result, errors.New("no failed sessions to retry")
			}
		}
		if explicit {
			status.Explicit = true
			status.Request = retry
			if err := saveState(*st); err != nil {
				return result, err
			}
		}
		result, err = runSessionBatch(ctx, c, req, func(id string, failed []sessions.BatchFailure) error {
			status.Failed = append(append([]sessions.BatchFailure(nil), outstanding...), failed...)
			status.Explicit = explicit
			status.Pending = id
			return saveState(*st)
		})
		result.Failed = append(result.Failed, outstanding...)
	}
	if errors.Is(err, errEmptySessionBatch) && !explicit {
		err = nil
	}
	if err != nil {
		if errors.Is(err, errAllSessionsRejected) {
			status.Pending = ""
			status.Explicit = false
		}
		if len(result.Failed) > 0 {
			status.Failed = result.Failed
		}
		if _, _, transient := sessionRetryDelay(err, 0); !transient && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			status.Explicit = false
		}
		status.Error = err.Error()
		return result, err
	}
	status.Error = ""
	status.Pending = ""
	status.Explicit = false
	status.Request = SessionRetry{}
	status.Failed = result.Failed
	if result.Remote && result.Backup != "" {
		status.Conversations = result.Conversations
		status.At = time.Now()
		status.Backup = result.Backup
	}
	status.Uploaded, status.Skipped = result.Uploaded, result.Skipped
	return result, nil
}
