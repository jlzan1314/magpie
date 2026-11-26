package davsync

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/yetone/magpie/internal/sessions"
)

var errAllSessionsRejected = errors.New("WebDAV rejected all conversations in this backup")

type sessionObjectRejected struct{ reason string }

func (e *sessionObjectRejected) Error() string {
	return "WebDAV rejected the session file: " + e.reason
}

// Only verified conversations enter the partial manifest. Keep the original
// local backup intact. The subset has its own content address and directory.
func (s batchStore) publishPartial(ctx context.Context, full batchManifest, rejected map[batchObject]error) (string, batchPublishStats, error) {
	var stats batchPublishStats
	sub := batchManifest{Version: 2, Device: full.Device, Origin: full.Origin}
	var objects []batchObject
	seen := map[batchObject]bool{}
	for i, id := range full.Entries {
		files := []batchObject{{"conversations", id}}
		parts := full.Parts[id]
		if len(parts) > 0 {
			files = nil
			for _, part := range parts {
				files = append(files, batchObject{"parts", part})
			}
		}
		var reason error
		for _, file := range files {
			if err := rejected[file]; err != nil {
				reason = err
				break
			}
		}
		info := full.Info[i]
		if reason != nil {
			for _, ref := range info.Sources {
				stats.failed = append(stats.failed, sessions.BatchFailure{Source: ref, Title: info.Title, Error: reason.Error()})
			}
			continue
		}
		sub.Entries = append(sub.Entries, id)
		sub.Info = append(sub.Info, info)
		if len(parts) > 0 {
			if sub.Parts == nil {
				sub.Parts = make(map[string][]string)
				sub.Version = 3
			}
			sub.Parts[id] = parts
		}
		for _, file := range files {
			if !seen[file] {
				seen[file] = true
				objects = append(objects, file)
			}
		}
	}
	if len(sub.Entries) == 0 {
		return s.batchID, stats, errAllSessionsRejected
	}
	data, err := json.Marshal(sub)
	if err != nil {
		return "", stats, err
	}
	target := s
	target.batchID = batchDigest(data)
	target.remoteDir = ""
	for _, o := range objects {
		from, to := s.filename(o.kind, o.id), target.filename(o.kind, o.id)
		if err := os.MkdirAll(filepath.Dir(to), 0700); err != nil {
			return "", stats, err
		}
		if err := os.Link(from, to); err != nil && !errors.Is(err, os.ErrExist) {
			raw, err := readBatchFile(from)
			if err != nil {
				return "", stats, err
			}
			if err := writeBatchFile(to, raw); err != nil {
				return "", stats, err
			}
		}
	}
	if _, err := target.keepLocal("backups", target.batchID, data); err != nil {
		return "", stats, err
	}
	id, sent, err := target.publishFrom(ctx, objects, sub, &s)
	if len(sent.failed) == 0 {
		sent.conversations = len(sub.Entries)
	}
	sent.failed = append(sent.failed, stats.failed...)
	return id, sent, err
}
