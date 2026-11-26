package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/yetone/magpie/internal/davsync"
	"github.com/yetone/magpie/internal/sessions"
)

func webdavSessionCmd(args []string) error {
	if len(args) > 0 && args[0] == "auto-import" {
		return webdavAutoImport(args[1:])
	}
	if len(args) == 1 && args[0] == "list" {
		return json.NewEncoder(os.Stdout).Encode(sessions.BatchCandidates())
	}
	if len(args) == 1 && args[0] == "backups" {
		out, err := davsync.ListSessionBackups(context.Background())
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	if len(args) == 1 && args[0] == "sync" {
		out, err := davsync.SyncLocalSessions(context.Background())
		if out.Backup != "" {
			if encodeErr := json.NewEncoder(os.Stdout).Encode(out); encodeErr != nil {
				return errors.Join(err, encodeErr)
			}
		}
		return err
	}
	if len(args) == 3 && args[0] == "delete" {
		return davsync.DeleteSessionBackup(context.Background(), args[1], args[2])
	}
	if len(args) != 0 && args[0] == "batch" {
		return webdavSessionBatch(args[1:])
	}
	if len(args) != 2 {
		return errors.New("usage: magpie webdav sessions list | backups | sync | auto-import | delete <id> local|webdav | batch [options] | push|pull <Codex session ID>")
	}
	res, err := davsync.TransferCodexSession(context.Background(), args[0], args[1])
	if err != nil {
		return err
	}
	if res.Changed == 0 {
		if res.Skipped > 0 {
			fmt.Println("Codex session matches remote packed size; skipped sync")
		} else {
			fmt.Println("Codex session already up to date")
		}
	} else {
		fmt.Println("Codex session transferred")
	}
	return nil
}

func webdavSessionBatch(args []string) error {
	var req davsync.SessionBatchRequest
	f := flag.NewFlagSet("magpie webdav sessions batch", flag.ContinueOnError)
	f.BoolVar(&req.All, "all", false, "include every readable conversation")
	f.StringVar(&req.Cwd, "cwd", "", "target project folder")
	f.Func("source", "select agent:id[@WSL-distro], repeatable", func(v string) error {
		agent, id, ok := strings.Cut(v, ":")
		if !ok || agent == "" || id == "" {
			return errors.New("session source must be agent:id")
		}
		id, wsl, _ := strings.Cut(id, "@")
		req.Sources = append(req.Sources, sessions.BatchSource{Agent: agent, ID: id, WSL: wsl})
		return nil
	})
	f.Func("backup", "merge a saved backup ID, repeatable", func(v string) error {
		req.Backups = append(req.Backups, v)
		return nil
	})
	f.Func("to", "import to these comma-separated agent IDs", func(v string) error {
		for _, t := range strings.Split(v, ",") {
			req.Targets = append(req.Targets, strings.TrimSpace(t))
		}
		return nil
	})
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("use --source or --backup to select conversations")
	}
	result, err := davsync.RunSessionBatch(context.Background(), req)
	if result.Backup != "" {
		if encodeErr := json.NewEncoder(os.Stdout).Encode(result); encodeErr != nil {
			return errors.Join(err, encodeErr)
		}
	}
	if err != nil {
		return err
	}
	for _, i := range result.Imports {
		if i.Error != "" {
			return errors.New("some agent imports need attention; see the returned results")
		}
	}
	return nil
}

// Configure the same per-device destinations and optional project folder as Settings.
func webdavAutoImport(args []string) error {
	prefs, err := davsync.LoadSessionPreferences()
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return json.NewEncoder(os.Stdout).Encode(struct {
			On      bool     `json:"on"`
			Targets []string `json:"targets"`
			Cwd     string   `json:"cwd,omitempty"`
		}{prefs.AutoImport, prefs.Targets, prefs.Cwd})
	}
	if len(args) == 1 && args[0] == "off" {
		prefs.AutoImport = false
		return davsync.SaveSessionPreferences(prefs)
	}
	flags := flag.NewFlagSet("magpie webdav sessions auto-import", flag.ContinueOnError)
	to := flags.String("to", "", "comma-separated target agents")
	cwd := flags.String("cwd", prefs.Cwd, "absolute destination project folder")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *to == "" {
		return errors.New("use auto-import --to codex,claude [--cwd path], or auto-import off")
	}
	targets := strings.Split(*to, ",")
	for i := range targets {
		targets[i] = strings.TrimSpace(targets[i])
	}
	if err = sessions.CheckBatchTargets(targets, *cwd); err != nil {
		return err
	}
	prefs.AutoImport, prefs.Targets, prefs.Cwd = true, targets, *cwd
	return davsync.SaveSessionPreferences(prefs)
}
