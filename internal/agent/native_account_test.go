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
	if !codexAgent.NativeAccount() || codexAgent.Routed() || codexAgent.Wired() {
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
	if err := codexAgent.SetNativeAccount(false); err != nil {
		t.Fatal(err)
	}
	if codexAgent.NativeAccount() || codexAgent.Routed() {
		t.Fatal("turning off native mode should leave routing available for explicit connect")
	}
}
