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

type SessionPreferences struct {
	AutoImport  bool     `json:"autoImport,omitempty"`
	ImportScope string   `json:"importScope,omitempty"`
	Targets     []string `json:"targets,omitempty"`
}

func sessionPreferencesPath() string { return path("session-batch-preferences.json") }

func LoadSessionPreferences() (SessionPreferences, error) {
	var prefs SessionPreferences
	raw, err := os.ReadFile(sessionPreferencesPath())
	if errors.Is(err, fs.ErrNotExist) {
		return prefs, nil
	}
	if err != nil {
		return prefs, err
	}
	if err := json.Unmarshal(raw, &prefs); err != nil {
		return SessionPreferences{}, err
	}
	if prefs.AutoImport {
		c, ok := Load()
		prefs.AutoImport = ok && prefs.ImportScope == stateKey(c)
	}
	return prefs, nil
}

func SaveSessionPreferences(prefs SessionPreferences) error {
	prefs.ImportScope = ""
	if prefs.AutoImport {
		c, ok := Load()
		if !ok || c.S3() || c.GitHub() || len(prefs.Targets) == 0 {
			return errors.New("choose a WebDAV server and an import target")
		}
		if err := sessions.CheckBatchTargets(prefs.Targets, ""); err != nil {
			return err
		}
		prefs.ImportScope = stateKey(c)
	}
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
