package library

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/tidwall/jsonc"
)

func TestMcporterTarget(t *testing.T) {
	h := sandbox(t)
	t.Setenv("MCPORTER_CONFIG", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("PATH", t.TempDir())

	if slices.Contains(ids(Targets()), "mcporter") {
		t.Fatalf("mcporter listed without config or binary: %v", ids(Targets()))
	}

	// Present via ~/.mcporter/mcporter.json (even without a global binary, e.g. npx mcporter).
	p := filepath.Join(h, ".mcporter", "mcporter.json")
	write(t, p, `{"mcpServers": {}, "imports": []}`)
	tg := targetByID("mcporter")
	if tg == nil || tg.MCP == nil || tg.MCP.Path != p || tg.MCP.Format != fmtMcporter {
		t.Fatalf("mcporter target: %+v", tg)
	}
	if tg.Instructions != "" || tg.Skills != "" {
		t.Fatalf("mcporter shouldn't take instructions or skills: %+v", tg)
	}
	for _, kind := range []string{"instructions", "skills"} {
		if _, err := Takes("mcporter", kind); err == nil {
			t.Fatalf("Takes(mcporter, %s) succeeded", kind)
		}
	}
	if id, err := Takes("mcporter", "mcp"); err != nil || id != "mcporter" {
		t.Fatalf("Takes(mcporter, mcp) = %q, %v", id, err)
	}

	// Falls back to mcporter.jsonc when mcporter.json doesn't exist.
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	jsoncPath := filepath.Join(h, ".mcporter", "mcporter.jsonc")
	write(t, jsoncPath, "// comment\n{\"mcpServers\": {},}\n")
	tg = targetByID("mcporter")
	if tg == nil || tg.MCP.Path != jsoncPath {
		t.Fatalf("expected jsonc path %s, got %+v", jsoncPath, tg)
	}

	// XDG_CONFIG_HOME/mcporter/mcporter.json takes precedence over ~/.mcporter.
	xdg := filepath.Join(h, "xdg")
	xdgPath := filepath.Join(xdg, "mcporter", "mcporter.json")
	write(t, xdgPath, `{"mcpServers": {}}`)
	t.Setenv("XDG_CONFIG_HOME", xdg)
	tg = targetByID("mcporter")
	if tg == nil || tg.MCP.Path != xdgPath {
		t.Fatalf("expected XDG path %s, got %+v", xdgPath, tg)
	}

	// Explicit MCPORTER_CONFIG takes top precedence.
	custom := filepath.Join(h, "custom", "my-mcporter.jsonc")
	write(t, custom, `{"mcpServers": {}}`)
	t.Setenv("MCPORTER_CONFIG", custom)
	tg = targetByID("mcporter")
	if tg == nil || tg.MCP.Path != custom {
		t.Fatalf("expected MCPORTER_CONFIG %s, got %+v", custom, tg)
	}
}

func TestMcporterMCP(t *testing.T) {
	h := sandbox(t)
	t.Setenv("MCPORTER_CONFIG", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	p := filepath.Join(h, ".mcporter", "mcporter.json")
	write(t, p, `{
  // existing mcporter config with JSONC comments and user fields
  "mcpServers": {
    "context7": {
      "baseUrl": "https://mcp.context7.com/mcp",
      "headers": {
        "X-Api-Key": "$env:CTX_KEY"
      },
      "description": "Docs lookup",
      "lifecycle": "keep-alive"
    },
    "bearer-alias": {
      "url": "https://api.example.com/mcp",
      "headers": {
        "Authorization": "Bearer old"
      },
      "bearerTokenEnv": "API_TOKEN"
    },
    "local-cmd": {
      "command": "npx -y \"@demo/mcp\" --flag",
      "env": {
        "TOKEN": "$env:LOCAL_TOKEN",
        "PLAIN": "${PLAIN_VAL}"
      },
      "cwd": "/tmp/demo"
    }
  },
  "imports": ["cursor"]
}`)

	v, err := Read(nil)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]*Server{}
	for _, f := range v.FoundServers {
		found[f.Server.Name] = f.Server
	}
	if ctx := found["context7"]; ctx == nil || ctx.Transport != "http" || ctx.URL != "https://mcp.context7.com/mcp" || ctx.Headers["X-Api-Key"] != "${CTX_KEY}" {
		t.Fatalf("decoded context7 = %+v", ctx)
	}
	if ba := found["bearer-alias"]; ba == nil || ba.URL != "https://api.example.com/mcp" || ba.Headers["Authorization"] != "${API_TOKEN}" {
		t.Fatalf("decoded bearer-alias = %+v", ba)
	}
	if lc := found["local-cmd"]; lc == nil || lc.Transport != "stdio" || lc.Command != "npx" || !slices.Equal(lc.Args, []string{"-y", "@demo/mcp", "--flag"}) || lc.Env["TOKEN"] != "${LOCAL_TOKEN}" || lc.Env["PLAIN"] != "${PLAIN_VAL}" {
		t.Fatalf("decoded local-cmd = %+v", lc)
	}

	// Import context7 into the library, then update it and verify user fields and top-level imports stay intact.
	if _, err := ImportServer("context7"); err != nil {
		t.Fatal(err)
	}
	res, err := SaveServer("context7", Server{
		Name:      "context7",
		Transport: "http",
		URL:       "https://mcp.context7.com/v2",
		Headers:   map[string]string{"X-Api-Key": "${CTX_KEY}"},
		Agents:    []string{"mcporter"},
	})
	if err != nil || len(res.Problems) > 0 {
		t.Fatalf("SaveServer context7: err=%v problems=%+v", err, res.Problems)
	}

	// Write a new stdio server and an SSE server to mcporter.
	res, err = SaveServer("", Server{
		Name:      "sse-feed",
		Transport: "sse",
		URL:       "https://sse.example.com/events",
		Headers:   map[string]string{"Authorization": "Bearer ${SSE_TOKEN}"},
		Agents:    []string{"mcporter"},
	})
	if err != nil || len(res.Problems) > 0 {
		t.Fatalf("SaveServer sse-feed: err=%v problems=%+v", err, res.Problems)
	}
	res, err = SaveServer("", Server{
		Name:      "stdio-new",
		Transport: "stdio",
		Command:   "uvx",
		Args:      []string{"demo-mcp"},
		Env:       map[string]string{"DEMO_KEY": "${DEMO_KEY}"},
		Agents:    []string{"mcporter"},
	})
	if err != nil || len(res.Problems) > 0 {
		t.Fatalf("SaveServer stdio-new: err=%v problems=%+v", err, res.Problems)
	}

	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(jsonc.ToJSON(raw), &doc); err != nil {
		t.Fatalf("parse mcporter.json: %v\n%s", err, raw)
	}
	imports, _ := doc["imports"].([]any)
	if len(imports) != 1 || imports[0] != "cursor" {
		t.Fatalf("top-level imports lost: %+v", doc["imports"])
	}
	servers, _ := doc["mcpServers"].(map[string]any)
	ctxEntry, _ := servers["context7"].(map[string]any)
	if ctxEntry["baseUrl"] != "https://mcp.context7.com/v2" || ctxEntry["description"] != "Docs lookup" || ctxEntry["lifecycle"] != "keep-alive" {
		t.Fatalf("context7 entry unexpected: %+v", ctxEntry)
	}
	if _, hasType := ctxEntry["type"]; hasType {
		t.Fatalf("mcporter entry shouldn't carry type: %+v", ctxEntry)
	}
	sseEntry, _ := servers["sse-feed"].(map[string]any)
	if sseEntry["baseUrl"] != "https://sse.example.com/events" {
		t.Fatalf("sse-feed entry unexpected: %+v", sseEntry)
	}

	// Re-reading should not report drift for the SSE server (MCPorter auto-falls back HTTP -> SSE).
	v, err = Read(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, sv := range v.Servers {
		if sv.Name == "sse-feed" && len(sv.Problems) > 0 {
			t.Fatalf("sse-feed reported problems: %+v", sv.Problems)
		}
	}

	// Removing stdio-new leaves other entries and imports intact.
	if _, err := RemoveServer("stdio-new"); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"stdio-new"`) || !strings.Contains(string(raw), `"local-cmd"`) {
		t.Fatalf("unexpected mcporter.json after remove:\n%s", raw)
	}
}

func TestMcporterProject(t *testing.T) {
	h := sandbox(t)
	t.Setenv("MCPORTER_CONFIG", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	write(t, filepath.Join(h, ".mcporter", "mcporter.json"), `{"mcpServers": {}}`)

	proj := filepath.Join(h, "repo")
	if err := os.MkdirAll(proj, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := AddProject(proj); err != nil {
		t.Fatal(err)
	}
	if _, err := SaveServer("", Server{
		Name:      "proj-srv",
		Transport: "http",
		URL:       "https://proj.example.com/mcp",
	}); err != nil {
		t.Fatal(err)
	}
	res, err := ProjectServer(proj, "proj-srv", []string{"mcporter"})
	if err != nil || len(res.Problems) > 0 {
		t.Fatalf("ProjectServer: err=%v problems=%+v", err, res.Problems)
	}
	cfgPath := filepath.Join(proj, "config", "mcporter.json")
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("expected project config at %s: %v", cfgPath, err)
	}
	if !strings.Contains(string(raw), `"baseUrl": "https://proj.example.com/mcp"`) {
		t.Fatalf("unexpected project config:\n%s", raw)
	}
}

func TestMcporterImportSelectsSourceAndKeepsBatchTogether(t *testing.T) {
	h := sandbox(t)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("MCPORTER_CONFIG", "")
	claudeConfig := filepath.Join(h, ".claude.json")
	write(t, claudeConfig, `{"mcpServers":{"shared":{"type":"stdio","command":"other"}}}`)
	write(t, filepath.Join(h, ".mcporter", "mcporter.json"), `{"mcpServers":{"shared":{"baseUrl":"https://mcp.example.com/"}}}`)
	if _, _, err := ImportServers([]string{"shared", "missing"}, "mcporter"); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("batch with a missing server: %v", err)
	}
	view, err := Read(nil)
	if err != nil || len(view.Servers) != 0 {
		t.Fatalf("failed import saved a server: %+v, %v", view, err)
	}
	imported, result, err := ImportServers(nil, "mcporter")
	if err != nil || result == nil || !slices.Equal(imported, []string{"shared"}) || len(result.Problems) > 0 {
		t.Fatalf("import from mcporter: %v, %+v, %v", imported, result, err)
	}
	view, err = Read(nil)
	if err != nil || len(view.Servers) != 1 || view.Servers[0].URL != "https://mcp.example.com/" || !slices.Equal(view.Servers[0].Agents, []string{"mcporter"}) {
		t.Fatalf("wrong server imported: %+v, %v", view, err)
	}
	if !strings.Contains(read(t, claudeConfig), `"command":"other"`) {
		t.Fatal("the unrelated Claude Code server changed")
	}
}

func TestMcporterCommandKeepsItsPath(t *testing.T) {
	format := &mcpFile{Format: fmtMcporter}
	for _, command := range []string{"/opt/Tool Directory/server", `C:\Program Files\MCP\server.exe`} {
		want := &Server{Transport: "stdio", Command: command}
		raw, err := json.Marshal(format.encode(want))
		if err != nil {
			t.Fatal(err)
		}
		var entry map[string]any
		if err := json.Unmarshal(raw, &entry); err != nil {
			t.Fatal(err)
		}
		if got, ok := format.decode("server", entry); !ok || !got.same(want) {
			t.Errorf("command %q round-tripped as %+v", command, got)
		}
	}
}

func TestMcporterCmdVars(t *testing.T) {
	f := mcpFile{Format: fmtMcporter}
	if got := f.cmdRefs(); got != refDollar {
		t.Fatalf("cmdRefs() = %v, want refDollar", got)
	}
	in := &Server{
		Name:      "demo",
		Transport: "stdio",
		Command:   "$HOME/bin/demo",
		Args:      []string{"--cfg", "${MY_CFG}"},
	}
	out := f.withCmdVars(in)
	if out.Command != "${HOME}/bin/demo" || !slices.Equal(out.Args, []string{"--cfg", "${MY_CFG}"}) {
		t.Fatalf("withCmdVars = %q %v", out.Command, out.Args)
	}
}
