package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"

	"github.com/Jenil133/Controlplane/internal/auth"
)

const tokenNote = `
The token is shown only this once and cannot be recovered. Add the entry to the
"tokens" list of the server's tokens file and restart the server; clients send
the token with cpctl --token or CPCTL_TOKEN.
`

// tokenCommand runs "token generate". It needs no server: servers only ever
// see token hashes, which they read from their tokens file.
func tokenCommand(out io.Writer, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("token: missing subcommand (generate); %w", errUsage)
	}
	if args[0] != "generate" {
		return fmt.Errorf("token: unknown subcommand %q; %w", args[0], errUsage)
	}
	fs := flag.NewFlagSet("token generate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	name := fs.String("name", "", "")
	role := fs.String("role", "", "")
	if _, err := parseArgs(fs, args[1:], 0); err != nil {
		return err
	}
	if *name == "" || *role == "" {
		return fmt.Errorf("token generate: --name and --role are required; %w", errUsage)
	}

	token, err := auth.GenerateToken()
	if err != nil {
		return err
	}
	entry := auth.Entry{Name: *name, Role: *role, TokenSHA256: auth.HashToken(token)}
	// Check the entry exactly as the server will when it loads the tokens
	// file, so that no token is shown for an entry it would refuse.
	if _, err := auth.New([]auth.Entry{entry}); err != nil {
		return fmt.Errorf("token generate: %w", err)
	}
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "token: %s\nentry: %s\n%s", token, line, tokenNote)
	return err
}
