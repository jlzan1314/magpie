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
	if len(args) == 3 && args[0] == "delete" {
		return davsync.DeleteSessionBackup(context.Background(), args[1], args[2])
	}
	if len(args) != 0 && args[0] == "batch" {
		return webdavSessionBatch(args[1:])
	}
	if len(args) != 2 {
		return errors.New("usage: magpie webdav sessions list | backups | delete <id> local|webdav | batch [options] | push|pull <Codex session ID>")
	}
	res, err := davsync.TransferCodexSession(context.Background(), args[0], args[1])
	if err != nil {
		return err
	}
	if res.Changed == 0 {
		fmt.Println("Codex session already up to date")
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
