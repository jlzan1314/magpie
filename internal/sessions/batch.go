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

type BatchFailure struct {
	Source BatchSource `json:"source"`
	Title  string      `json:"title,omitempty"`
	Error  string      `json:"error"`
}

// CollectBatch preserves the strict behavior for callers that require all sources.
func CollectBatch(ctx context.Context, refs []BatchSource, all bool) ([]BatchConversation, error) {
	entries, _, err := collectBatch(ctx, refs, all, false, false)
	return entries, err
}

// CollectAvailableBatch returns readable transcripts and identifies individual
// failures so unrelated conversations can still reach a verified snapshot.
func CollectAvailableBatch(ctx context.Context, refs []BatchSource, all bool) ([]BatchConversation, []BatchFailure, error) {
	return collectBatch(ctx, refs, all, true, false)
}

// CollectOutgoingBatch leaves out unedited native copies created by an import.
func CollectOutgoingBatch(ctx context.Context, refs []BatchSource, all bool) ([]BatchConversation, []BatchFailure, error) {
	return collectBatch(ctx, refs, all, true, true)
}

func collectBatch(ctx context.Context, refs []BatchSource, all, partial, outgoing bool) ([]BatchConversation, []BatchFailure, error) {
	var out []BatchConversation
	failed, err := VisitBatch(ctx, refs, all, partial, outgoing, nil, func(c BatchConversation, _ string) error { out = append(out, c); return nil })
	return out, failed, err
}

// VisitBatch opens one conversation at a time; a settled single-file
// transcript can reuse a previously verified revision without reading its body.
func VisitBatch(ctx context.Context, refs []BatchSource, all, partial, outgoing bool, skip func(BatchSource, string) bool, visit func(BatchConversation, string) error) ([]BatchFailure, error) {
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
	var failed []BatchFailure
	for _, ref := range sortedSources(refs) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s, ok := src[ref]
		mark := ""
		if ok && skip != nil {
			mark = batchSourceMark(s)
			if skip(ref, mark) {
				continue
			}
		}
		var c BatchConversation
		var err error
		if !ok || !Reads(s.Agent) {
			err = errors.New("session is no longer available")
		} else {
			c, err = readBatchConversation(ctx, ref, s)
			if err == nil {
				err = c.validate()
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !all || !errors.Is(err, errBatchEmpty) {
				failure := fmt.Errorf("reading %s:%s: %w", ref.Agent, ref.ID, err)
				if !partial {
					return nil, failure
				}
				failed = append(failed, BatchFailure{ref, s.Title, failure.Error()})
			}
			continue
		}
		if outgoing && unchangedImport(s, c) {
			continue
		}
		if err := visit(c, mark); err != nil {
			return nil, err
		}
	}
	return failed, nil
}

func batchSourceMark(s Session) string {
	// Database-backed and composite readers may change outside the main
	// file. Always use their content fingerprint.
	if s.Agent != "codex" && s.Agent != "claude" || s.BranchOf != "" {
		return ""
	}
	mark := SettledFileMark(s.Path)
	if mark == "" {
		return ""
	}
	data, _ := json.Marshal([]any{s.Path, mark, s.Title, s.Cwd, s.Start})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// SettledFileMark changes on a rewrite or replacement even when mtime is
// restored. Unsupported or too-recent timestamps must be checked by content.
func SettledFileMark(path string) string {
	begin := time.Now()
	info, err := codexDiscoveryStat(path)
	if err != nil || !info.Mode().IsRegular() || !settledCodexStamp(info, begin) {
		return ""
	}
	return fmt.Sprintf("%d/%d/%d", info.Size(), info.ModTime().UnixNano(), codexChangeTime(info).UnixNano())
}

func readBatchConversation(ctx context.Context, ref BatchSource, s Session) (BatchConversation, error) {
	c := BatchConversation{Sources: []BatchSource{ref}, Title: s.Title, Cwd: s.Cwd, Start: s.Start}
	if !Reads(s.Agent) {
		return c, ErrNotRead
	}
	var readErr error
	err := readTranscript(s, func(_ bool, p Part) bool {
		if readErr = ctx.Err(); readErr != nil {
			return false
		}
		p.Model = ""
		if strings.TrimSpace(p.Text) == "" || p.Kind == "context" || p.Kind == "thinking" {
			return true
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
	projects := make([]string, len(entries))
	for i, c := range entries {
		project, err := batchProject(cwd, c.Cwd)
		if err != nil {
			return nil, err
		}
		projects[i] = project
	}
	existing := knownBatchTargets(ctx, entries, targets, projects)
	items := make([][]BatchImport, len(targets))
	var wg sync.WaitGroup
	for i, target := range targets {
		wg.Go(func() {
			for j, c := range entries {
				if ctx.Err() != nil {
					return
				}
				items[i] = append(items[i], importBatchItem(c, target, projects[j], existing))
			}
		})
	}
	wg.Wait()
	return slices.Concat(items...), ctx.Err()
}

type batchDestination struct{ agent, cwd, fingerprint string }

func knownBatchTargets(ctx context.Context, entries []BatchConversation, targets, projects []string) map[batchDestination]Session {
	wanted := map[string]map[string]bool{}
	for i, c := range entries {
		project := filepath.Clean(projects[i])
		if wanted[project] == nil {
			wanted[project] = map[string]bool{}
		}
		wanted[project][c.Fingerprint()] = true
	}
	return indexBatchTargets(ctx, targets, wanted)
}

// BatchReceiver indexes local transcripts once, then imports a remote snapshot
// a conversation at a time without retaining the decoded history of the batch.
type BatchReceiver struct {
	targets []string
	cwd     string
	known   map[batchDestination]Session
}

func NewBatchReceiver(ctx context.Context, targets []string, cwd string) (*BatchReceiver, error) {
	if len(targets) == 0 {
		return nil, errors.New("choose one or more target agents")
	}
	if err := CheckBatchTargets(targets, cwd); err != nil {
		return nil, err
	}
	targets = slices.Compact(slices.Sorted(slices.Values(targets)))
	known := indexBatchTargets(ctx, targets, nil)
	return &BatchReceiver{targets: targets, cwd: cwd, known: known}, ctx.Err()
}

func (r *BatchReceiver) Receive(ctx context.Context, c BatchConversation) (int, error) {
	project, err := batchProject(r.cwd, c.Cwd)
	if err != nil {
		return 0, err
	}
	count := 0
	var failures []error
	for _, target := range r.targets {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		result := importBatchItem(c, target, project, r.known)
		if result.Error != "" {
			failures = append(failures, fmt.Errorf("importing into %s: %s", target, result.Error))
		} else if !result.Existing {
			count++
		}
	}
	return count, errors.Join(failures...)
}

func indexBatchTargets(ctx context.Context, targets []string, wanted map[string]map[string]bool) map[batchDestination]Session {
	known := map[batchDestination]Session{}
	for ref, s := range batchSources() {
		if ctx.Err() != nil {
			break
		}
		project := filepath.Clean(s.Cwd)
		if ref.WSL != "" || (wanted != nil && wanted[project] == nil) || !slices.Contains(targets, ref.Agent) {
			continue
		}
		c, err := readBatchConversation(ctx, ref, s)
		if err != nil {
			continue
		}
		sum := c.Fingerprint()
		key := batchDestination{s.Agent, project, sum}
		if old, found := known[key]; (wanted == nil || wanted[project][sum]) && (!found || s.ID < old.ID) {
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

func batchProject(override, original string) (string, error) {
	if override != "" {
		return override, nil
	}
	if batchDirectory(original) {
		return original, nil
	}
	return os.UserHomeDir()
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
	receipt := batchImportReceipt{CarryResult: r}
	// Hash the native representation, since cross-agent conversion can change
	// tool items. Later edits will then be exported as ordinary local changes.
	native := Session{Agent: target, ID: r.ID, Path: r.Path, Cwd: cwd}
	if created, e := readBatchConversation(context.Background(), BatchSource{Agent: target, ID: r.ID}, native); e == nil {
		receipt.Fingerprint = created.Fingerprint()
	}
	raw, err := json.Marshal(receipt)
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

type batchImportReceipt struct {
	CarryResult
	Fingerprint string `json:"fingerprint,omitempty"`
}

func unchangedImport(s Session, c BatchConversation) bool {
	if _, err := uuid.Parse(s.ID); err != nil || s.WSL != "" {
		return false
	}
	raw, err := os.ReadFile(filepath.Join(settings.Dir(), "session-imports", s.ID+".json"))
	if err != nil {
		return false
	}
	var saved batchImportReceipt
	return json.Unmarshal(raw, &saved) == nil && saved.Fingerprint != "" && saved.Agent == s.Agent && saved.Path == s.Path && saved.Fingerprint == c.Fingerprint()
}
