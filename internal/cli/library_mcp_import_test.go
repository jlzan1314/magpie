package cli

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/yetone/magpie/internal/library"
)

func TestLibraryMCPImportFromMcporter(t *testing.T) {
	h := t.TempDir()
	t.Setenv("HOME", h)
	t.Setenv("USERPROFILE", h)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("MCPORTER_CONFIG", "")

	mcPath := filepath.Join(h, ".mcporter", "mcporter.json")
	if err := os.MkdirAll(filepath.Dir(mcPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mcPath, []byte(`{
  "mcpServers": {
    "context7": {
      "baseUrl": "https://mcp.context7.com/mcp"
    },
    "local-tool": {
      "command": "npx",
      "args": ["-y", "demo-mcp"]
    }
  },
  "imports": []
}`), 0o600); err != nil {
		t.Fatal(err)
	}

	out, err := runOut(t, "library", "mcp", "import", "--from=mcporter")
	if err != nil {
		t.Fatalf("import --from=mcporter failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "imported 2 MCP servers") {
		t.Fatalf("unexpected output:\n%s", out)
	}

	v, err := library.Read(nil)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(v.Servers))
	for _, s := range v.Servers {
		names = append(names, s.Name)
		if !slices.Contains(s.Agents, "mcporter") {
			t.Fatalf("server %s missing mcporter agent: %v", s.Name, s.Agents)
		}
	}
	if !slices.Contains(names, "context7") || !slices.Contains(names, "local-tool") {
		t.Fatalf("imported servers = %v", names)
	}

	// Running again reports nothing left to import.
	out, err = runOut(t, "library", "mcp", "import", "--from=mcporter")
	if err != nil {
		t.Fatalf("second import failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "no unimported MCP servers found in mcporter") {
		t.Fatalf("unexpected second output:\n%s", out)
	}
}
