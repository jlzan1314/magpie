package agent

import (
	"errors"
	"strings"
	"sync"
)

const (
	nativeAccountKey         = ".native_account"
	nativeAccountDisabledKey = ".native_account_disabled"
)

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

func (a *Agent) Enabled() bool {
	if a.NativeAccount() {
		return stashShared()[a.ID+nativeAccountDisabledKey] != "1"
	}
	return a.Wired()
}

func (a *Agent) EnableHow() (Connection, error) {
	if !a.SupportsNativeAccount() {
		return a.ConnectHow()
	}
	nativeAccountMu.Lock()
	defer nativeAccountMu.Unlock()
	if a.NativeAccount() {
		if err := a.Disconnect(); err != nil {
			return Connection{}, err
		}
		return Connection{How: "native-account"}, setNativeAccountState(a.ID, true, true)
	}
	return a.ConnectHow()
}

func (a *Agent) Disable() error {
	if !a.SupportsNativeAccount() {
		return a.Disconnect()
	}
	nativeAccountMu.Lock()
	defer nativeAccountMu.Unlock()
	if a.NativeAccount() {
		if err := a.Disconnect(); err != nil {
			return err
		}
		return setNativeAccountState(a.ID, true, false)
	}
	return a.Disconnect()
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
	native, enabled := a.NativeAccount(), a.Enabled()
	if !on && !native {
		return nil
	}
	if err := setNativeAccountState(a.ID, on, true); err != nil {
		return err
	}
	var err error
	if on {
		err = a.Disconnect()
	} else if enabled {
		err = a.Connect()
	}
	if err != nil {
		return errors.Join(err, setNativeAccountState(a.ID, native, enabled))
	}
	return nil
}

func setNativeAccountState(id string, native, enabled bool) error {
	stashMu.Lock()
	defer stashMu.Unlock()
	stash := stashLoad()
	if native {
		stash[id+nativeAccountKey] = "1"
	} else {
		delete(stash, id+nativeAccountKey)
	}
	if native && !enabled {
		stash[id+nativeAccountDisabledKey] = "1"
	} else {
		delete(stash, id+nativeAccountDisabledKey)
	}
	return stashWrite(stash)
}
