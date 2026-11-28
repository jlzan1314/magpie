package agent

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/yetone/magpie/internal/edit"
)

const (
	codexCustomID    = "custom"
	codexCustomTable = "model_providers." + codexCustomID
	codexCustomBase  = codexCustomTable + ".base_url"
)

type codexCustomRoute struct {
	path string
	key  string
}

type codexCustomOriginal struct {
	Gateway         string
	PreviousGateway string
	Fields          map[string]*edit.TOMLText
}

func (r codexCustomRoute) selected() bool {
	p, _ := edit.GetTOMLTop(r.path, "model_provider")
	return p == codexCustomID
}

func (r codexCustomRoute) managed() bool {
	return stashShared()[r.key] != ""
}

func (o codexCustomOriginal) owns(base string) bool {
	return base != "" && (base == o.Gateway || base == o.PreviousGateway)
}

func (r codexCustomRoute) routed() bool {
	if !r.selected() {
		return false
	}
	var original codexCustomOriginal
	if raw := stashShared()[r.key]; raw == "" || json.Unmarshal([]byte(raw), &original) != nil {
		return false
	}
	base, _ := edit.GetTOML(r.path, codexCustomBase)
	return original.owns(base)
}

func (r codexCustomRoute) available() bool {
	if !r.selected() {
		return false
	}
	if r.routed() {
		return true
	}
	doc, err := codexTopOf(r.path)
	if err != nil {
		return false
	}
	providers, _ := doc.ModelProviders.(map[string]any)
	table, _ := providers[codexCustomID].(map[string]any)
	if table["requires_openai_auth"] != true || table["wire_api"] != "responses" {
		return false
	}
	// A custom relay can use its own credentials. Only take over the alias
	// of Codex's native account, whose credentials stay in Codex.
	for _, k := range []string{"auth", "env_key", "experimental_bearer_token", "http_headers", "env_http_headers", "query_params"} {
		if _, ok := table[k]; ok {
			return false
		}
	}
	switch base, _ := table["base_url"].(string); strings.TrimSuffix(base, "/") {
	case "", "https://chatgpt.com/backend-api/codex", "https://chatgpt.com/backend-api":
		return true
	}
	return false
}

func (r codexCustomRoute) take(gateway, token string, openAIAuth bool, top ...edit.KV) error {
	values := []edit.KV{
		{Path: codexCustomBase, Value: gateway},
		{Path: codexCustomTable + ".experimental_bearer_token", Value: token},
		{Path: codexCustomTable + ".requires_openai_auth", Value: openAIAuth},
		{Path: codexCustomTable + ".supports_websockets", Value: false},
		{Path: codexCustomTable + ".http_headers", Value: map[string]any{"x-openai-actor-authorization": "magpie"}},
	}

	stashMu.Lock()
	defer stashMu.Unlock()
	saved := stashLoad()
	var original codexCustomOriginal
	if raw := saved[r.key]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &original); err != nil {
			return fmt.Errorf("read the original Codex custom provider: %w", err)
		}
	}
	base, _ := edit.GetTOML(r.path, codexCustomBase)
	if !original.owns(base) {
		if !r.available() {
			return fmt.Errorf("Codex's custom provider changed outside Magpie")
		}
		original = codexCustomOriginal{Fields: make(map[string]*edit.TOMLText, len(values))}
		for _, field := range values {
			if v, ok := edit.GetTOMLText(r.path, field.Path); ok {
				original.Fields[field.Path] = &v
			} else {
				original.Fields[field.Path] = nil
			}
		}
	} else if gateway != original.Gateway {
		original.PreviousGateway = base
	}
	original.Gateway = gateway
	raw, err := json.Marshal(original)
	if err != nil {
		return err
	}
	if string(raw) != saved[r.key] {
		saved[r.key] = string(raw)
		if err := stashWrite(saved); err != nil {
			return err
		}
	}
	return edit.SetTOML(r.path, append(values, top...)...)
}

func (r codexCustomRoute) restore(dropCatalog bool) error {
	stashMu.Lock()
	defer stashMu.Unlock()
	raw := stashShared()[r.key]
	if raw == "" {
		return nil
	}
	var original codexCustomOriginal
	if err := json.Unmarshal([]byte(raw), &original); err != nil {
		return fmt.Errorf("read the original Codex custom provider: %w", err)
	}
	if _, err := codexTopOf(r.path); err != nil {
		return err
	}
	// A replacement from outside Magpie owns the provider table now. Keep
	// it and remove the catalog reference only if it is still Magpie's.
	base, _ := edit.GetTOML(r.path, codexCustomBase)
	var changes []edit.KV
	if original.owns(base) {
		for path, v := range original.Fields {
			field := edit.KV{Path: path}
			if v != nil {
				field.Value = *v
			}
			changes = append(changes, field)
		}
	}
	if dropCatalog {
		changes = append(changes, edit.KV{Path: "model_catalog_json"})
	}
	if len(changes) == 0 {
		return nil
	}
	// Keep this snapshot until the next takeover. If a later config write
	// fails and rolls back the restore, the same snapshot is still usable.
	return edit.SetTOML(r.path, changes...)
}
