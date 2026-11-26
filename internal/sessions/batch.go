package sessions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/yetone/magpie/internal/filelock"
	"github.com/yetone/magpie/internal/settings"
	"github.com/yetone/magpie/internal/steady"
)

const MaxBatchConversation = 64 << 20

// BatchSource identifies a native conversation without accepting paths from a caller.
type BatchSource struct {
	Agent string `json:"agent"`
	ID    string `json:"id"`
	WSL   string `json:"wsl,omitempty"`
}

type BatchCandidate struct {
	BatchSource
	Title string    `json:"title"`
	Cwd   string    `json:"cwd"`
	Last  time.Time `json:"last"`
}

// BatchConversation is a portable transcript with its source references.
// It contains no account state or terminal configuration.
type BatchConversation struct {
	Sources []BatchSource `json:"sources"`
	Title   string        `json:"title"`
	Cwd     string        `json:"cwd"`
	Start   time.Time     `json:"start"`
	Parts   []Part        `json:"parts"`
}

func batchSources() map[BatchSource]Session {
	out := map[BatchSource]Session{}
	for _, s := range List(math.MaxInt) {
		if readable(s) && !s.Gateway && s.Path != "" && Reads(s.Agent) {
			out[BatchSource{s.Agent, s.ID, s.WSL}] = s
		}
	}
	return out
}

func BatchCandidates() []BatchCandidate {
	src := batchSources()
	out := make([]BatchCandidate, 0, len(src))
	for ref, s := range src {
		out = append(out, BatchCandidate{ref, s.Title, s.Cwd, s.Last})
	}
	slices.SortFunc(out, func(a, b BatchCandidate) int { return b.Last.Compare(a.Last) })
	return out
}

func CollectBatch(ctx context.Context, refs []BatchSource, all bool) ([]BatchConversation, error) {
	if all && len(refs) != 0 {
		return nil, errors.New("choose all sessions or individual sessions")
	}
	if !all && len(refs) == 0 {
		return nil, nil
	}
	src := batchSources()
	if all {
		refs = slices.Collect(maps.Keys(src))
	}
	out := make([]BatchConversation, 0, len(refs))
	for _, ref := range sortedSources(refs) {
		s, ok := src[ref]
		if !ok || !Reads(s.Agent) {
			return nil, fmt.Errorf("session %s:%s is no longer available", ref.Agent, ref.ID)
		}
		c, err := readBatchConversation(ctx, ref, s)
		if err != nil {
			return nil, fmt.Errorf("reading %s:%s: %w", ref.Agent, ref.ID, err)
		}

		if err := c.validate(); err != nil {
			if all && errors.Is(err, errBatchEmpty) {
				continue
			}
			return nil, fmt.Errorf("reading %s:%s: %w", ref.Agent, ref.ID, err)
		}
		out = append(out, c)
	}
	return out, nil
}

func readBatchConversation(ctx context.Context, ref BatchSource, s Session) (BatchConversation, error) {
	c := BatchConversation{Sources: []BatchSource{ref}, Title: s.Title, Cwd: s.Cwd, Start: s.Start}
	if !Reads(s.Agent) {
		return c, ErrNotRead
	}
	var size int
	var readErr error
	err := readTranscript(s, func(_ bool, p Part) bool {
		if readErr = ctx.Err(); readErr != nil {
			return false
		}
		p.Model = ""
		if strings.TrimSpace(p.Text) == "" || p.Kind == "context" || p.Kind == "thinking" {
			return true
		}
		size += len(p.Text) + len(p.Name) + 64
		if size > MaxBatchConversation {
			readErr = errors.New("session exceeds the 64 MiB uncompressed transcript limit")
			return false
		}
		c.Parts = append(c.Parts, p)
		return true
	})
	return c, errors.Join(err, readErr)
}

var errBatchEmpty = errors.New("session has no readable user message")

func (c BatchConversation) validate() error {
	if len(c.Sources) == 0 {
		return errors.New("session has no source")
	}
	for _, p := range c.Parts {
		if p.Role == "user" && p.Kind == "text" && strings.TrimSpace(p.Text) != "" {
			return nil
		}
	}
	return errBatchEmpty
}

func sortedSources(refs []BatchSource) []BatchSource {
	out := slices.Clone(refs)
	slices.SortFunc(out, func(a, b BatchSource) int {
		if a.Agent != b.Agent {
			return strings.Compare(a.Agent, b.Agent)
		}
		if a.ID != b.ID {
			return strings.Compare(a.ID, b.ID)
		}
		return strings.Compare(a.WSL, b.WSL)
	})
	return slices.Compact(out)
}

func (c BatchConversation) Fingerprint() string {
	b, _ := json.Marshal(c.Parts)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// MergeBatch combines equal transcripts from any reader. It fast-forwards
// shorter revisions only when they share a source ID and agree on every part.
// Two divergent histories stay separate.
func MergeBatch(in []BatchConversation) []BatchConversation {
	exact := map[string]BatchConversation{}
	for _, c := range in {
		key := c.Fingerprint()
		if old, ok := exact[key]; ok {
			c = combineBatchSources(old, c)
		}
		exact[key] = c
	}
	keys := slices.Collect(maps.Keys(exact))
	slices.SortFunc(keys, func(a, b string) int {
		na, nb := len(exact[a].Parts), len(exact[b].Parts)
		if na != nb {
			return nb - na
		}
		return strings.Compare(a, b)
	})
	bySource := map[BatchSource][]int{}
	var out []BatchConversation
	record := func(i int, refs []BatchSource) {
		for _, ref := range refs {
			if !slices.Contains(bySource[ref], i) {
				bySource[ref] = append(bySource[ref], i)
			}
		}
	}
	for _, key := range keys {
		c := exact[key]
		related := map[int]bool{}
		for _, ref := range c.Sources {
			for _, i := range bySource[ref] {
				if len(out[i].Parts) > len(c.Parts) && slices.Equal(out[i].Parts[:len(c.Parts)], c.Parts) {
					related[i] = true
				}
			}
		}
		if len(related) == 0 {
			record(len(out), c.Sources)
			out = append(out, c)
		}
		for i := range related {
			out[i] = combineBatchSources(out[i], c)
			record(i, c.Sources)
		}
	}

	return out
}

func combineBatchSources(keep, also BatchConversation) BatchConversation {
	keep.Sources = sortedSources(append(slices.Clone(keep.Sources), also.Sources...))
	return keep
}

// BatchImportTargets are the formats Magpie can write as native sessions.
func BatchImportTargets() []string { return []string{"claude", "codex"} }

type BatchImport struct {
	Agent    string `json:"agent"`
	Session  string `json:"session,omitempty"`
	Command  string `json:"command,omitempty"`
	Existing bool   `json:"existing,omitempty"`
	Error    string `json:"error,omitempty"`
}

func CheckBatchTargets(targets []string, cwd string) error {
	for _, t := range targets {
		if !slices.Contains(BatchImportTargets(), t) {
			return fmt.Errorf("native session import is unavailable for %s", t)
		}
	}
	if cwd != "" && !batchDirectory(cwd) {
		return errors.New("the target project must be an existing absolute directory")
	}
	return nil
}

// ImportBatch distributes the same batch to each selected target in one
// operation. A target's failure can be retried from the preserved backup.
func ImportBatch(ctx context.Context, entries []BatchConversation, targets []string, cwd string) ([]BatchImport, error) {
	if len(targets) == 0 {
		return nil, errors.New("choose one or more target agents")
	}
	if err := CheckBatchTargets(targets, cwd); err != nil {
		return nil, err
	}
	targets = slices.Compact(slices.Sorted(slices.Values(targets)))
	existing := knownBatchTargets(ctx, entries, targets, cwd)
	items := make([][]BatchImport, len(targets))
	var wg sync.WaitGroup
	for i, target := range targets {
		wg.Go(func() {
			for _, c := range entries {
				if ctx.Err() != nil {
					return
				}
				project := cwd
				if project == "" {
					project = c.Cwd
				}
				items[i] = append(items[i], importBatchItem(c, target, project, existing))
			}
		})
	}
	wg.Wait()
	return slices.Concat(items...), ctx.Err()
}

type batchDestination struct{ agent, cwd, fingerprint string }

func knownBatchTargets(ctx context.Context, entries []BatchConversation, targets []string, cwd string) map[batchDestination]Session {
	wanted := map[string]map[string]bool{}
	for _, c := range entries {
		path := cwd
		if path == "" {
			path = c.Cwd
		}
		project := filepath.Clean(path)
		if wanted[project] == nil {
			wanted[project] = map[string]bool{}
		}
		wanted[project][c.Fingerprint()] = true
	}
	known := map[batchDestination]Session{}
	for ref, s := range batchSources() {
		if ctx.Err() != nil {
			break
		}
		project := filepath.Clean(s.Cwd)
		if ref.WSL != "" || wanted[project] == nil || !slices.Contains(targets, ref.Agent) {
			continue
		}
		c, err := readBatchConversation(ctx, ref, s)
		if err != nil {
			continue
		}
		sum := c.Fingerprint()
		key := batchDestination{s.Agent, project, sum}
		if old, found := known[key]; wanted[project][sum] && (!found || s.ID < old.ID) {
			known[key] = s
		}
	}
	return known
}

func importBatchItem(c BatchConversation, target, cwd string, existing map[batchDestination]Session) BatchImport {
	r := BatchImport{Agent: target}
	if !batchDirectory(cwd) {
		r.Error = "the source project is unavailable here; choose a target project"
		return r
	}
	if old, ok := existing[batchDestination{target, filepath.Clean(cwd), c.Fingerprint()}]; ok {
		r.Session, r.Command, r.Existing = old.ID, resumeCommand("", target, old.ID, cwd, old.Path), true
		return r
	}
	s, seen, err := importBatchConversation(c, target, cwd)
	if err != nil {
		r.Error = err.Error()
		return r
	}
	r.Session, r.Command, r.Existing = s.ID, s.Command, seen
	return r
}

func batchDirectory(p string) bool {
	if !filepath.IsAbs(p) {
		return false
	}
	info, err := os.Stat(p)
	return err == nil && info.IsDir()
}

func importBatchConversation(c BatchConversation, target, cwd string) (CarryResult, bool, error) {
	if err := c.validate(); err != nil {
		return CarryResult{}, false, err
	}
	key := c.Fingerprint() + "\x00" + target + "\x00" + cwd
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(key)).String()
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
	var r CarryResult
	if raw, err := os.ReadFile(save); err == nil && json.Unmarshal(raw, &r) == nil {
		if fi, err := os.Stat(r.Path); err == nil && fi.Mode().IsRegular() {
			return r, true, nil
		}
	}
	items := batchCarryItems(c.Parts)
	now := c.Start.UTC()
	if now.IsZero() {
		now = time.Unix(0, 0).UTC()
	}
	var err error
	switch target {
	case "claude":
		r, err = writeClaudeCarryID(cwd, items, now, id)
	case "codex":
		r, err = writeCodexCarryID(CodexDir(), cwd, items, now, id)
	}
	already := false
	if errors.Is(err, fs.ErrExist) {
		already, err = true, nil // the native writer completed before a receipt was saved
	}
	if err != nil {
		return CarryResult{}, false, err
	}
	r.Command = resumeCommand("", target, r.ID, cwd, r.Path)
	raw, err := json.Marshal(r)
	if err == nil {
		err = steady.WriteFile(save, raw, 0o600)
	}
	return r, already, err
}

func batchCarryItems(parts []Part) []carryItem {
	out := make([]carryItem, 0, len(parts))
	for _, p := range parts {
		switch {
		case p.Role == "user" && p.Kind == "text":
			out = append(out, carryItem{Kind: "user", Text: p.Text})
		case p.Role == "assistant" && p.Kind == "text":
			out = append(out, carryItem{Kind: "assistant", Text: p.Text})
		default:
			text := strings.TrimSpace(p.Kind + " " + p.Name)
			out = append(out, carryItem{Kind: "assistant", Text: "[" + text + "]\n" + p.Text})
		}
	}
	return out
}
