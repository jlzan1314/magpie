package agent

import (
	"errors"
	"strings"
	"sync"
)

const nativeAccountKey = ".native_account"

var nativeAccountMu sync.Mutex

var ErrNativeAccount = errors.New("this agent uses its native account; turn off native account mode to apply settings from magpie")

func NativeAccountEnabled(id string) bool {
	return stashShared()[id+nativeAccountKey] == "1"
}

func (a *Agent) SupportsNativeAccount() bool {
	return a.ID == "codex" || strings.HasPrefix(a.ID, "codex@wsl:")
}

func (a *Agent) NativeAccount() bool {
	return a.SupportsNativeAccount() && NativeAccountEnabled(a.ID)
}

func (a *Agent) SetNativeAccount(on bool) error {
	if !a.SupportsNativeAccount() {
		return errors.New("native account mode is unavailable for this agent")
	}
	if on && a.WSL != "" && !WSLRunning(a.WSL) {
		return errors.New("start the WSL distro before changing native account mode")
	}
	nativeAccountMu.Lock()
	defer nativeAccountMu.Unlock()
	already := a.NativeAccount()
	if !on && !already {
		return nil
	}
	if err := setNativeAccountMark(a.ID, on); err != nil {
		return err
	}
	if on {
		if err := a.Disconnect(); err != nil {
			if !already {
				return errors.Join(err, setNativeAccountMark(a.ID, false))
			}
			return err
		}
	}
	return nil
}

func setNativeAccountMark(id string, on bool) error {
	stashMu.Lock()
	defer stashMu.Unlock()
	stash := stashLoad()
	if on {
		stash[id+nativeAccountKey] = "1"
	} else {
		delete(stash, id+nativeAccountKey)
	}
	return stashWrite(stash)
}
