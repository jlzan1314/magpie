package davsync

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/yetone/magpie/internal/sessions"
)

// SessionActivity is shared by windows on this device. Byte counts are for
// sealed objects whose remote content has been verified or safely reused.
type SessionActivity struct {
	Imported      int                     `json:"imported,omitempty"`
	ImportError   string                  `json:"importError,omitempty"`
	ImportedAt    time.Time               `json:"importedAt,omitzero"`
	Sending       int64                   `json:"sending,omitempty"`
	Conversations int                     `json:"conversations"`
	Uploaded      int                     `json:"uploaded"`
	Skipped       int                     `json:"skipped"`
	Phase         string                  `json:"phase"`
	Bytes         int64                   `json:"bytes"`
	Total         int64                   `json:"total"`
	Objects       int                     `json:"objects"`
	Count         int                     `json:"count"`
	RetryAt       time.Time               `json:"retryAt,omitzero"`
	CanRetry      bool                    `json:"canRetry,omitempty"`
	Error         string                  `json:"error,omitempty"`
	Latest        string                  `json:"latest,omitempty"`
	Last          time.Time               `json:"last,omitzero"`
	Failed        []sessions.BatchFailure `json:"failed,omitempty"`
}

type SessionRetry struct {
	Now        bool                  `json:"now,omitempty"`
	FailedOnly bool                  `json:"failedOnly"`
	Source     *sessions.BatchSource `json:"source,omitempty"`
}

type sessionJob struct {
	mu     sync.Mutex
	view   SessionActivity
	done   chan struct{}
	wake   chan struct{}
	result SessionBatchResult
	err    error
}

type sessionJobKey struct{}

var sessionJobs = struct {
	sync.Mutex
	byKey map[string]*sessionJob
}{byKey: make(map[string]*sessionJob)}

func beginSessionJob(c Config) (*sessionJob, bool) {
	sessionJobs.Lock()
	defer sessionJobs.Unlock()
	key := path(stateKey(c))
	if job := sessionJobs.byKey[key]; job != nil {
		select {
		case <-job.done:
		default:
			return job, false
		}
	}
	job := &sessionJob{done: make(chan struct{}), wake: make(chan struct{}, 1), view: SessionActivity{Phase: "preparing"}}
	sessionJobs.byKey[key] = job
	return job, true
}

func (j *sessionJob) finish(r SessionBatchResult, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.view.Sending = 0
	j.view.CanRetry = false
	j.view.RetryAt = time.Time{}
	j.result, j.err = r, err
	j.view.Conversations, j.view.Uploaded, j.view.Skipped = r.Conversations, r.Uploaded, r.Skipped
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		j.view.Phase = "pending"
	case err != nil:
		j.view.Phase = "failed"
		j.view.Error = err.Error()
	case j.view.Phase == "importing" && r.Backup == "" && len(r.Failed) == 0:
		j.view.Phase = "idle"
	case len(r.Failed) > 0:
		j.view.Phase = "partial"
	default:
		j.view.Phase = "done"
	}
	close(j.done)
}

func updateSessionActivity(ctx context.Context, update func(*SessionActivity)) {
	if j, ok := ctx.Value(sessionJobKey{}).(*sessionJob); ok {
		j.mu.Lock()
		update(&j.view)
		j.mu.Unlock()
	}
}

func SessionSyncActivity() SessionActivity {
	c, ok := Load()
	v := SessionActivity{Phase: "idle"}
	if !ok {
		return v
	}
	key := stateKey(c)
	sessionJobs.Lock()
	job := sessionJobs.byKey[path(key)]
	sessionJobs.Unlock()
	if job != nil {
		job.mu.Lock()
		v = job.view
		job.mu.Unlock()
	}
	st := loadState()
	if st.Key == key && st.Sessions != nil {
		s := st.Sessions
		v.Imported, v.ImportedAt, v.ImportError = s.Imported, s.ImportedAt, sessionImportDiagnostic(s.ImportError)
		v.Latest, v.Last, v.Failed = s.Backup, s.At, s.Failed
		if job == nil {
			v.Conversations, v.Uploaded, v.Skipped = s.Conversations, s.Uploaded, s.Skipped
			v.Error = s.currentError()
			switch {
			case s.Pending != "" && (s.interrupted() || s.Error == ""):
				v.Phase = "pending"
			case v.Error != "":
				v.Phase = "failed"
			case len(v.Failed) > 0:
				v.Phase = "partial"
			case s.Pending != "":
				v.Phase = "pending"
			}
		}
	}
	// Explicit backup actions also publish this hint after manifest verification.
	if v.Latest == currentSessionID {
		return v
	}
	hint := batchHint(c)
	if raw, err := os.ReadFile(hint); err == nil {
		id := strings.TrimSpace(string(raw))
		if (id != v.Latest || v.Last.IsZero()) && validBatchID(id) {
			if fi, err := os.Stat(hint); err == nil {
				v.Latest, v.Last = id, fi.ModTime()
			}
		} else if id == "" {
			v.Latest = ""
			v.Last = time.Time{}
		}
	}
	return v
}

// StartSessionSync accepts an explicit request once, even across page reloads.
func StartSessionSync(retry SessionRetry) (SessionActivity, error) {
	c, err := sessionUploadConfig()
	if err != nil {
		return SessionActivity{}, err
	}
	job, owner := beginSessionJob(c)
	if owner {
		go func() {
			r, err := syncLocalSessionJob(context.Background(), c, job, retry)
			job.finish(r, err)
		}()
	} else {
		job.mu.Lock()
		if retry.Now && job.view.CanRetry {
			select {
			case job.wake <- struct{}{}:
			default:
			}
		}
		job.mu.Unlock()
	}
	return SessionSyncActivity(), nil
}

type temporarySessionHTTP struct{ status string }

func (e *temporarySessionHTTP) Error() string { return "WebDAV: " + e.status }

func sessionRetryDelay(err error, attempt int) (time.Duration, bool, bool) {
	delay := time.Second * 2 << attempt
	var rate *rateLimited
	if errors.As(err, &rate) {
		if rate.after > 0 {
			return max(delay, rate.after), true, true
		}
		return delay, false, true
	}
	var status *temporarySessionHTTP
	var stalled *errStalled
	var op *net.OpError
	var dns *net.DNSError
	transient := errors.As(err, &status) || errors.As(err, &stalled) || errors.As(err, &op) || errors.As(err, &dns) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
	return delay, false, transient
}

func retrySessionOperation(ctx context.Context, op func() error) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := op()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil || attempt >= 4 {
			return err
		}
		delay, serverWait, retry := sessionRetryDelay(err, attempt)
		if !retry {
			return err
		}
		var wake <-chan struct{}
		if job, ok := ctx.Value(sessionJobKey{}).(*sessionJob); ok && !serverWait {
			wake = job.wake
		}
		updateSessionActivity(ctx, func(a *SessionActivity) {
			a.Phase = "waiting"
			a.RetryAt = time.Now().Add(delay)
			a.CanRetry = !serverWait
		})
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-wake:
			timer.Stop()
		case <-timer.C:
		}
		updateSessionActivity(ctx, func(a *SessionActivity) { a.Phase = "uploading"; a.CanRetry = false; a.RetryAt = time.Time{} })
	}
}
