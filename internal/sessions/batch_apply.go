package sessions

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/yetone/magpie/internal/filelock"
	"github.com/yetone/magpie/internal/settings"
	"github.com/yetone/magpie/internal/steady"
)

func applyBatchConversation(ctx context.Context, c BatchConversation, target, cwd string, old batchTarget) (CarryResult, bool, error) {
	if err := c.validate(); err != nil {
		return CarryResult{}, false, err
	}
	identity := c.SyncIdentity()
	now := c.Start.UTC()
	if now.IsZero() {
		now = time.Unix(0, 0).UTC()
	}
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte("session\x00"+identity+"\x00"+target+"\x00"+cwd)).String()
	if old.ID != "" {
		id = old.ID
	}
	var path string
	if target == "codex" {
		path = filepath.Join(CodexDir(), "sessions", codexCarryPath(now, id))
	} else {
		project, err := claudeProjectName(cwd)
		if err != nil {
			return CarryResult{}, false, err
		}
		path = filepath.Join(ClaudeDir(), "projects", project, id+".jsonl")
	}
	if old.Path != "" {
		path = old.Path
	}
	dir := filepath.Join(settings.Dir(), "session-imports")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return CarryResult{}, false, err
	}
	save := filepath.Join(dir, id+".json")
	unlock, ok := filelock.Lock(save+".lock", filelock.Wait)
	if !ok {
		return CarryResult{}, false, errors.New("another Magpie is importing the conversation")
	}
	defer unlock()
	result := CarryResult{Agent: target, ID: id, Path: path, Command: resumeCommand("", target, id, cwd, path)}
	native := Session{Agent: target, ID: id, Path: path, Cwd: cwd}
	ref := BatchSource{Agent: target, ID: id}
	saved, managed := findBatchImport(native)
	items := batchCarryItems(c.Parts)
	var projected BatchConversation
	for _, item := range items {
		if strings.TrimSpace(item.Text) != "" {
			projected.Parts = append(projected.Parts, textPart(item.Kind, item.Text))
		}
	}
	next := batchImportReceipt{CarryResult: result, Identity: identity, Title: c.Title, Start: c.Start, Fingerprint: projected.Fingerprint()}
	fi, err := os.Lstat(path)
	exists := err == nil
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return result, false, err
	}
	var codex CodexRollout
	var claude []byte
	if exists {
		if !fi.Mode().IsRegular() {
			return result, false, errors.New("the import path is not a regular session file")
		}
		if target == "codex" {
			files, _, err := readCodexSnapshot(ctx, filepath.Join(CodexDir(), "sessions"), id)
			if err != nil {
				return result, false, err
			}
			if len(files) != 1 {
				return result, false, ErrCodexSyncConflict
			}
			codex = files[0]
		} else if claude, err = os.ReadFile(path); err != nil {
			return result, false, err
		}
		current, err := readBatchConversation(ctx, ref, native)
		if err != nil {
			return result, false, err
		}
		fingerprint := current.Fingerprint()
		if fingerprint == next.Fingerprint {
			return result, true, saveBatchImportReceipt(save, next)
		}
		settled := managed && (saved.Fingerprint == fingerprint || saved.NextFingerprint == fingerprint)
		if !settled && (old.fingerprint != fingerprint || c.Last.Before(old.Last)) {
			return result, false, errors.New("the imported session has local changes; sync them before applying another revision")
		}
		if managed {
			saved.Fingerprint, saved.NextFingerprint = fingerprint, next.Fingerprint
			if err := saveBatchImportReceipt(save, saved); err != nil {
				return result, false, err
			}
		}
	} else {
		if err := saveBatchImportReceipt(save, next); err != nil {
			return result, false, err
		}
	}
	if target == "codex" {
		if exists {
			header, _, _ := bytes.Cut(codex.Data, []byte{'\n'})
			lines := append([][]byte{header}, codexCarryBody(cwd, items, now)...)
			body := append(bytes.Join(lines, []byte{'\n'}), '\n')
			_, err = importCodexSnapshot(ctx, id, []CodexRollout{{Path: codex.Path, Data: body}}, &codex)
		} else {
			err = writeNew(path, codexCarryLines(CodexDir(), cwd, items, now, id))
		}
	} else {
		lines := claudeCarryLines(cwd, items, now, id)
		if exists {
			current, err := os.ReadFile(path)
			if err != nil {
				return result, false, err
			}
			if !bytes.Equal(current, claude) {
				return result, false, errors.New("Claude Code changed the session while it was being imported")
			}
			body := append(bytes.Join(lines, []byte{'\n'}), '\n')
			if err := steady.WriteFile(path, body, 0o600); err != nil {
				return result, false, err
			}
		} else {
			err = writeNew(path, lines)
		}
	}
	if err != nil {
		return result, false, err
	}
	forgetWarmListing()
	if written, err := readBatchConversation(ctx, ref, native); err == nil {
		next.Fingerprint = written.Fingerprint()
	}
	return result, false, saveBatchImportReceipt(save, next)
}
