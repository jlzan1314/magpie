package agent

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yetone/magpie/internal/edit"
)

func TestCodexNativeAccountRestoresCustomProvider(t *testing.T) {
	for _, base := range []string{"", "https://chatgpt.com/backend-api/codex/"} {
		t.Run(base, func(t *testing.T) {
			const model = "gpt-5.4"
			config := "model_provider = \"custom\"\nmodel = \"" + model + "\"\n\n" +
				strings.Replace(ccSwitchMirrorTable, `name = "OpenAI"`, `name = "OpenAI legacy alias"`, 1)
			if base != "" {
				config += "base_url = \"" + base + "\"\n"
			}
			home, read := codexHome(t, chatGPTAuth, config)
			dir := filepath.Join(home, ".codex")
			const id = "019a0000-0000-7000-8000-0000000000c1"
			path := codexThreadOn(t, dir, id, "custom")
			rollout, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			authPath := filepath.Join(dir, "auth.json")

			cx := codex(home)
			providerTable := "model_providers.custom"
			before, err := edit.GetTOMLTable(cx.Path, providerTable)
			if err != nil {
				t.Fatal(err)
			}
			for _, on := range []bool{false, true, false, true} {
				cx = codex(home)
				if !on && !cx.NativeAccount() {
					err = cx.Pick("model", "fake/m1")
				} else {
					err = cx.SetNativeAccount(on)
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := cx.Sync(); err != nil {
					t.Fatal(err)
				}
				active, _ := edit.GetTOMLTop(cx.Path, "model_provider")
				if active != "custom" || !cx.Enabled() || cx.NativeAccount() != on || cx.Routed() == on {
					t.Fatalf("native=%v: provider or agent state changed:\n%s", on, read())
				}
				table, err := edit.GetTOMLTable(cx.Path, providerTable)
				if err != nil {
					t.Fatal(err)
				}
				if on {
					if got, _ := edit.GetTOMLTop(cx.Path, "model"); got != model || !reflect.DeepEqual(table, before) {
						t.Fatalf("native settings were not restored:\n%s", read())
					}
				} else if table["base_url"] != here(home).v1() || table["name"] != before["name"] || cx.Check() != "" {
					t.Fatalf("custom did not route through magpie: %s\n%s", cx.Check(), read())
				}
				if !on {
					codexUsedUp = func() bool { return true }
					if err := cx.Sync(); err != nil {
						t.Fatal(err)
					}
					if auth, _ := edit.GetTOML(cx.Path, providerTable+".requires_openai_auth"); auth != "false" {
						t.Fatalf("custom still requires OpenAI auth after the account ran out:\n%s", read())
					}
					codexUsedUp = func() bool { return false }
				}
				if current, err := os.ReadFile(path); err != nil || string(current) != string(rollout) || codexRowOn(t, dir, id) != "custom" {
					t.Fatalf("switch rewrote the original thread: %v", err)
				}
				if current, err := os.ReadFile(authPath); err != nil || string(current) != chatGPTAuth {
					t.Fatalf("switch modified the Codex sign-in: %v", err)
				}
			}
		})
	}
}

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
