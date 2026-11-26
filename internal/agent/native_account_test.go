package agent

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexNativeAccount(t *testing.T) {
	const auth = `{"tokens":{"access_token":"x","id_token":"x.e30.x"}}`
	home, read := codexHome(t, auth, "")
	codexAgent := codex(home)
	if err := codexAgent.Fields[0].Set("fake/m1"); err != nil {
		t.Fatal(err)
	}
	if !codexAgent.Routed() {
		t.Fatal("fixture should route through magpie")
	}
	if err := codexAgent.SetNativeAccount(true); err != nil {
		t.Fatal(err)
	}
	codexAgent = codex(home)
	if !codexAgent.Enabled() || !codexAgent.NativeAccount() || codexAgent.Routed() || codexAgent.Wired() {
		t.Fatalf("Codex did not return to its native route: %s", read())
	}
	config := read()
	if strings.Contains(config, "openai_base_url") {
		t.Fatalf("magpie proxy remains in config: %s", config)
	}
	codexUsedUp = func() bool { return true }
	if err := codexAgent.Sync(); err != nil || read() != config {
		t.Fatalf("background sync changed the native config: %v", err)
	}
	if err := codexAgent.Connect(); !errors.Is(err, ErrNativeAccount) {
		t.Fatalf("connect in native mode: %v", err)
	}
	gotAuth, err := os.ReadFile(filepath.Join(codexAgent.Dir, "auth.json"))
	if err != nil || string(gotAuth) != auth {
		t.Fatalf("native sign-in was not preserved: %v", err)
	}
	if err := codexAgent.Disable(); err != nil {
		t.Fatal(err)
	}
	if codexAgent.Enabled() || !codexAgent.NativeAccount() || read() != config {
		t.Fatal("disabling Codex should retain the native account preference and config")
	}
	if _, err := codexAgent.EnableHow(); err != nil {
		t.Fatal(err)
	}
	if !codexAgent.Enabled() || !codexAgent.NativeAccount() || read() != config {
		t.Fatal("enabling Codex again should keep the native route and config")
	}
	codexUsedUp = func() bool { return false }
	if err := codexAgent.SetNativeAccount(false); err != nil {
		t.Fatal(err)
	}
	if codexAgent.NativeAccount() || !codexAgent.Enabled() || !codexAgent.Routed() {
		t.Fatal("turning off native mode should return the enabled agent to Magpie routing")
	}
}
