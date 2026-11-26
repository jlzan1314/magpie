package davsync

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/yetone/magpie/internal/lastgood"
	"github.com/yetone/magpie/internal/sessions"
	"github.com/yetone/magpie/internal/steady"
)

// SessionPreferences holds this device's draft for the session batch form.
// A draft may be saved before the user has chosen to run any transfer.
type SessionPreferences struct {
	All         bool                   `json:"all"`
	Sources     []sessions.BatchSource `json:"sources,omitempty"`
	SourceAgent string                 `json:"sourceAgent,omitempty"`
	Search      string                 `json:"search,omitempty"`
	BackupIDs   string                 `json:"backupIDs,omitempty"`
	Targets     []string               `json:"targets,omitempty"`
	Cwd         string                 `json:"cwd,omitempty"`
}

func sessionPreferencesPath() string { return path("session-batch-preferences.json") }

func LoadSessionPreferences() (SessionPreferences, error) {
	prefs := SessionPreferences{All: true}
	raw, err := os.ReadFile(sessionPreferencesPath())
	if errors.Is(err, fs.ErrNotExist) {
		return prefs, nil
	}
	if err != nil {
		return prefs, err
	}
	if err := json.Unmarshal(raw, &prefs); err != nil {
		return SessionPreferences{All: true}, err
	}
	return prefs, nil
}

func SaveSessionPreferences(prefs SessionPreferences) error {
	file := sessionPreferencesPath()
	if err := lastgood.KeepUnparsed(file, lastgood.Into[SessionPreferences]); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(prefs, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		return err
	}
	return steady.WriteFile(file, append(raw, '\n'), 0o600)
}
