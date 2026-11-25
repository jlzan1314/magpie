//go:build nogui

package cli

import "errors"

const hasGUI = false

func runGUI(bool, string) error {
	return errors.New("this build has no GUI; run `magpie web` for browser settings, `magpie daemon install --web` for a background service, or `magpie tui`")
}

func runPanel() error { return runGUI(false, "") }

func runWindow(string) error { return runGUI(true, "") }

func runUninstallWindow() error { return runGUI(true, "") }
