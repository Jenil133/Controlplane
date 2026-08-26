// Command cpctl is a command-line client for the control plane.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
)

// actorHeader must match server.ActorHeader.
const actorHeader = "x-controlplane-actor"

const usage = `cpctl - command-line client for the control plane

Usage:
  cpctl [global flags] <command> [arguments]
  cpctl help [command]       (also: cpctl <command> --help)

Entries:
  ns create <name> [--description text]
  ns get <name>
  ns list
  config put <namespace> <key> <value> [--description text]
  config delete <namespace> <key>
  flag put <namespace> <key> [--enabled] [--rollout percent] [--salt s] [--allow u1,u2]...
           [--description text]
  flag delete <namespace> <key>
  experiment put <namespace> <key> --variants a=50,b=50 [--payload b=<json>]... [--enabled]
                 [--salt s] [--description text]
  experiment delete <namespace> <key>
  ratelimit put <namespace> <key> --rps n --burst n [--enabled] [--description text]
  ratelimit delete <namespace> <key>
  breaker put <namespace> <key> --failure-rate x --min-requests n --window d --open-duration d
              --half-open-requests n [--enabled] [--description text]
  breaker delete <namespace> <key>

Staged rollouts:
  rollout start <namespace> <flag> --stages 1:10m,5:10m,25:30m,100
  rollout advance|pause|resume|abort <namespace> <flag>
  rollout status <namespace> <flag>

History, audit and rollback:
  history <namespace> [--limit n] [--before revision]
  revision <namespace> <revision>
  diff <namespace> <from-revision> [to-revision] [--json]
  rollback <namespace> <revision> [--yes] [--expected-revision n]
  audit [namespace] [--type t] [--key k] [--actor name] [--since t] [--until t]
        [--limit n] [--page-token t] [--json]

Clients:
  snapshot <namespace>
  watch <namespace> [--known-revision n] [--full]
  eval <namespace> <unit>

Tokens:
  token generate --name name --role reader|editor|admin

Config values and payloads are parsed as JSON; anything that is not valid JSON
is sent as a string. Use -- before a value that starts with a dash.

Every put and delete also takes --expected-revision n, and then fails without
changing anything unless the entry is still at revision n. flag put replaces
the enabled state, description and allowlist (--allow takes comma-separated
unit IDs and may be repeated); the rollout percentage and salt keep their
current values unless given.

Durations use Go syntax: 500ms, 10s, 5m, 1h30m. Rollout stages are
percent[:duration], comma-separated: the controller advances a stage once its
duration has passed, a stage without one waits for "rollout advance", and
reaching the last stage completes the rollout.

diff prints one line per change, "+" added, "~" modified, "-" removed, from
the first revision to the second (default: the current state). rollback
restores the state a namespace had at an earlier revision, as a new revision;
rollouts that were active then come back paused. It only previews the changes
unless --yes is given. The preview prints the --expected-revision to apply it
with, so that applying fails if the namespace has changed in between. audit
--since and --until take RFC 3339 times or durations ago (24h).

eval shows how every flag and experiment of a namespace evaluates for one unit,
exactly as the SDK evaluates them. token generate needs no server: it prints a
new token, once, and the entry to add to the server's tokens file.

Global flags go before the command:
  --addr string      control plane gRPC address (env CPCTL_ADDR, default localhost:9090)
  --token string     bearer token for a server with auth enabled (env CPCTL_TOKEN);
                     prefer the variable, command lines are visible to other users
  --actor string     name recorded on changes (env CPCTL_ACTOR, default $USER); a
                     server with auth enabled records the token's name instead
  --timeout duration per-request timeout, 0 for none (default 10s)

Flags of a command may come before, between or after its arguments. Exit status
is 0 on success, 2 for a command line that cpctl rejects, and 1 for any other
failure; gRPC failures print the status code name, e.g. "NotFound: ...".
`

// errUsage marks command lines cpctl rejects before talking to a server.
var errUsage = errors.New("see 'cpctl help'")

// errHelp is returned by flag parsing when --help was given; run turns it
// into the command's usage and a zero exit status.
var errHelp = errors.New("help requested")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "cpctl:", errorText(err))
		stop()
		os.Exit(exitCode(err))
	}
}

// errorText renders err for the terminal. gRPC failures lead with the name
// of their status code, which is what scripts and docs refer to.
func errorText(err error) string {
	if s, ok := status.FromError(err); ok && s.Code() != codes.OK {
		return fmt.Sprintf("%s: %s", s.Code(), s.Message())
	}
	return err.Error()
}

// exitCode is 2 for a rejected command line, as for most Unix tools, and 1
// for everything else.
func exitCode(err error) int {
	if errors.Is(err, errUsage) {
		return 2
	}
	return 1
}

type cli struct {
	admin   cpv1.AdminServiceClient
	dist    cpv1.DistributionServiceClient
	actor   string
	timeout time.Duration
	out     io.Writer
}

func run(ctx context.Context, args []string, out io.Writer) error {
	global := flag.NewFlagSet("cpctl", flag.ContinueOnError)
	global.SetOutput(io.Discard)
	addr := global.String("addr", envOr("CPCTL_ADDR", "localhost:9090"), "")
	token := global.String("token", envOr("CPCTL_TOKEN", ""), "")
	actor := global.String("actor", envOr("CPCTL_ACTOR", os.Getenv("USER")), "")
	timeout := global.Duration("timeout", 10*time.Second, "")
	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(out, usage)
			return nil
		}
		return fmt.Errorf("%w; %w", err, errUsage)
	}
	args = global.Args()
	if len(args) == 0 {
		fmt.Fprint(out, usage)
		return nil
	}
	if args[0] == "help" {
		if len(args) > 1 {
			return printCommandUsage(out, args[1])
		}
		fmt.Fprint(out, usage)
		return nil
	}
	if len(args) > 1 && isHelpFlag(args[1]) {
		return printCommandUsage(out, args[0])
	}
	if *timeout < 0 {
		return fmt.Errorf("--timeout must not be negative; %w", errUsage)
	}
	err := dispatch(ctx, args, out, *addr, strings.TrimSpace(*token), *actor, *timeout)
	if errors.Is(err, errHelp) {
		return printCommandUsage(out, args[0])
	}
	return err
}

// dispatch runs one command. Token generation needs no server; everything
// else dials one lazily, so a command line cpctl rejects never waits on the
// network.
func dispatch(ctx context.Context, args []string, out io.Writer, addr, token, actor string, timeout time.Duration) error {
	if args[0] == "token" {
		return tokenCommand(out, args[1:])
	}

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if token != "" {
		// cpctl speaks plaintext (localhost, port-forwards, in-cluster), so
		// the credentials must not insist on TLS.
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(auth.TokenCredentials(token, false)))
	}
	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	c := &cli{
		admin:   cpv1.NewAdminServiceClient(conn),
		dist:    cpv1.NewDistributionServiceClient(conn),
		actor:   actor,
		timeout: timeout,
		out:     out,
	}

	switch cmd, rest := args[0], args[1:]; cmd {
	case "ns", "namespace":
		return c.namespace(ctx, rest)
	case "config":
		return c.config(ctx, rest)
	case "flag":
		return c.flag(ctx, rest)
	case "experiment", "exp":
		return c.experiment(ctx, rest)
	case "ratelimit", "rate-limit":
		return c.rateLimit(ctx, rest)
	case "breaker", "circuit-breaker":
		return c.breaker(ctx, rest)
	case "rollout":
		return c.rollout(ctx, rest)
	case "history":
		return c.history(ctx, rest)
	case "revision":
		return c.revision(ctx, rest)
	case "diff":
		return c.diff(ctx, rest)
	case "rollback":
		return c.rollback(ctx, rest)
	case "audit":
		return c.audit(ctx, rest)
	case "snapshot":
		return c.snapshot(ctx, rest)
	case "watch":
		return c.watch(ctx, rest)
	case "eval":
		return c.evaluate(ctx, rest)
	default:
		return fmt.Errorf("unknown command %q; %w", cmd, errUsage)
	}
}

// commandAliases maps the spellings run accepts to the name usage uses.
var commandAliases = map[string]string{
	"namespace": "ns", "exp": "experiment", "rate-limit": "ratelimit", "circuit-breaker": "breaker",
}

func isHelpFlag(s string) bool {
	return s == "-h" || s == "-help" || s == "--help" || s == "help"
}

// printCommandUsage prints the lines of the usage text that describe one
// command, so that it cannot drift from the full reference.
func printCommandUsage(out io.Writer, command string) error {
	if name, ok := commandAliases[command]; ok {
		command = name
	}
	var synopsis []string
	matched := false
	for _, l := range strings.Split(usage, "\n") {
		switch {
		case strings.HasPrefix(l, "  ") && !strings.HasPrefix(l, "   ") && !strings.HasPrefix(l, "  -"):
			// An entry starts at the usual indent; so does a global flag line.
			word, _, _ := strings.Cut(strings.TrimSpace(l), " ")
			matched = word == command
		case !strings.HasPrefix(l, "   "):
			matched = false
		}
		if matched {
			synopsis = append(synopsis, l)
		}
	}
	if len(synopsis) == 0 {
		return fmt.Errorf("unknown command %q; %w", command, errUsage)
	}
	fmt.Fprintf(out, "Usage:\n%s\n\nRun 'cpctl help' for what the arguments and flags mean.\n", strings.Join(synopsis, "\n"))
	return nil
}

func (c *cli) call(ctx context.Context) (context.Context, context.CancelFunc) {
	cancel := context.CancelFunc(func() {})
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
	}
	if c.actor != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, actorHeader, c.actor)
	}
	return ctx, cancel
}

// print writes m as indented JSON. protojson deliberately varies its
// whitespace, so the output is re-indented to keep it stable.
func (c *cli) print(m proto.Message) error {
	b, err := protojson.Marshal(m)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, b, "", "  "); err != nil {
		return err
	}
	buf.WriteByte('\n')
	_, err = buf.WriteTo(c.out)
	return err
}

func (c *cli) deleted(kind, key, namespace string, revision int64) {
	fmt.Fprintf(c.out, "deleted %s %s (namespace %s now at revision %d)\n", kind, key, namespace, revision)
}

func (c *cli) namespace(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("ns: missing subcommand (create, get, list); %w", errUsage)
	}
	fs := flag.NewFlagSet("ns "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	description := fs.String("description", "", "")
	ctx, cancel := c.call(ctx)
	defer cancel()

	switch args[0] {
	case "create":
		pos, err := parseArgs(fs, args[1:], 1)
		if err != nil {
			return err
		}
		resp, err := c.admin.CreateNamespace(ctx, &cpv1.CreateNamespaceRequest{Name: pos[0], Description: *description})
		if err != nil {
			return err
		}
		return c.print(resp.GetNamespace())
	case "get":
		pos, err := parseArgs(fs, args[1:], 1)
		if err != nil {
			return err
		}
		resp, err := c.admin.GetNamespace(ctx, &cpv1.GetNamespaceRequest{Name: pos[0]})
		if err != nil {
			return err
		}
		return c.print(resp.GetNamespace())
	case "list":
		if _, err := parseArgs(fs, args[1:], 0); err != nil {
			return err
		}
		resp, err := c.admin.ListNamespaces(ctx, &cpv1.ListNamespacesRequest{})
		if err != nil {
			return err
		}
		tw := newTable(c.out)
		fmt.Fprintln(tw, "NAME\tREVISION\tUPDATED\tDESCRIPTION")
		for _, ns := range resp.GetNamespaces() {
			fmt.Fprintf(tw, "%s\t%d\t%s\t%s\n", clean(ns.GetName()), ns.GetRevision(), formatTime(ns.GetUpdatedAt()), clean(ns.GetDescription()))
		}
		return tw.Flush()
	default:
		return fmt.Errorf("ns: unknown subcommand %q; %w", args[0], errUsage)
	}
}

func (c *cli) config(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("config: missing subcommand (put, delete); %w", errUsage)
	}
	fs := flag.NewFlagSet("config "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	description := fs.String("description", "", "")
	expected := fs.Int64("expected-revision", 0, "")
	ctx, cancel := c.call(ctx)
	defer cancel()

	switch args[0] {
	case "put":
		pos, err := parseArgs(fs, args[1:], 3)
		if err != nil {
			return err
		}
		value, err := parseValue(pos[2])
		if err != nil {
			return err
		}
		resp, err := c.admin.PutConfig(ctx, &cpv1.PutConfigRequest{
			Namespace: pos[0], Key: pos[1], Value: value, Description: *description, ExpectedRevision: *expected,
		})
		if err != nil {
			return err
		}
		return c.print(resp.GetConfig())
	case "delete":
		pos, err := parseArgs(fs, args[1:], 2)
		if err != nil {
			return err
		}
		resp, err := c.admin.DeleteConfig(ctx, &cpv1.DeleteConfigRequest{Namespace: pos[0], Key: pos[1], ExpectedRevision: *expected})
		if err != nil {
			return err
		}
		c.deleted("config", pos[1], pos[0], resp.GetRevision())
		return nil
	default:
		return fmt.Errorf("config: unknown subcommand %q; %w", args[0], errUsage)
	}
}

func (c *cli) flag(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("flag: missing subcommand (put, delete); %w", errUsage)
	}
	fs := flag.NewFlagSet("flag "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	description := fs.String("description", "", "")
	enabled := fs.Bool("enabled", false, "")
	salt := fs.String("salt", "", "")
	expected := fs.Int64("expected-revision", 0, "")
	var allowlist stringList
	fs.Var(&allowlist, "allow", "")
	// The server keeps the current percentage unless one is sent, so an
	// absent --rollout must stay distinguishable from --rollout 0.
	var rolloutPercent *float64
	fs.Func("rollout", "", func(s string) error {
		p, err := parsePercent(s)
		if err != nil {
			return err
		}
		rolloutPercent = &p
		return nil
	})
	ctx, cancel := c.call(ctx)
	defer cancel()

	switch args[0] {
	case "put":
		pos, err := parseArgs(fs, args[1:], 2)
		if err != nil {
			return err
		}
		resp, err := c.admin.PutFlag(ctx, &cpv1.PutFlagRequest{
			Namespace:        pos[0],
			Key:              pos[1],
			Enabled:          *enabled,
			Description:      *description,
			RolloutPercent:   rolloutPercent,
			Salt:             *salt,
			Allowlist:        allowlist,
			ExpectedRevision: *expected,
		})
		if err != nil {
			return err
		}
		return c.print(resp.GetFlag())
	case "delete":
		pos, err := parseArgs(fs, args[1:], 2)
		if err != nil {
			return err
		}
		resp, err := c.admin.DeleteFlag(ctx, &cpv1.DeleteFlagRequest{Namespace: pos[0], Key: pos[1], ExpectedRevision: *expected})
		if err != nil {
			return err
		}
		c.deleted("flag", pos[1], pos[0], resp.GetRevision())
		return nil
	default:
		return fmt.Errorf("flag: unknown subcommand %q; %w", args[0], errUsage)
	}
}

func (c *cli) experiment(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("experiment: missing subcommand (put, delete); %w", errUsage)
	}
	fs := flag.NewFlagSet("experiment "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	description := fs.String("description", "", "")
	enabled := fs.Bool("enabled", false, "")
	salt := fs.String("salt", "", "")
	variants := fs.String("variants", "", "")
	expected := fs.Int64("expected-revision", 0, "")
	payloads := keyValues{}
	fs.Var(payloads, "payload", "")
	ctx, cancel := c.call(ctx)
	defer cancel()

	switch args[0] {
	case "put":
		pos, err := parseArgs(fs, args[1:], 2)
		if err != nil {
			return err
		}
		vs, err := parseVariants(*variants, payloads)
		if err != nil {
			return err
		}
		resp, err := c.admin.PutExperiment(ctx, &cpv1.PutExperimentRequest{
			Namespace: pos[0], Key: pos[1], Enabled: *enabled, Salt: *salt, Description: *description, Variants: vs,
			ExpectedRevision: *expected,
		})
		if err != nil {
			return err
		}
		return c.print(resp.GetExperiment())
	case "delete":
		pos, err := parseArgs(fs, args[1:], 2)
		if err != nil {
			return err
		}
		resp, err := c.admin.DeleteExperiment(ctx, &cpv1.DeleteExperimentRequest{Namespace: pos[0], Key: pos[1], ExpectedRevision: *expected})
		if err != nil {
			return err
		}
		c.deleted("experiment", pos[1], pos[0], resp.GetRevision())
		return nil
	default:
		return fmt.Errorf("experiment: unknown subcommand %q; %w", args[0], errUsage)
	}
}

func (c *cli) snapshot(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	pos, err := parseArgs(fs, args, 1)
	if err != nil {
		return err
	}
	ctx, cancel := c.call(ctx)
	defer cancel()
	resp, err := c.dist.GetSnapshot(ctx, &cpv1.GetSnapshotRequest{Namespace: pos[0]})
	if err != nil {
		return err
	}
	return c.print(resp.GetSnapshot())
}

// watch prints one line per pushed snapshot. since_change is the time between
// the write being committed and the snapshot arriving here, i.e. the
// end-to-end propagation latency for live changes (clocks permitting).
func (c *cli) watch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	known := fs.Int64("known-revision", 0, "")
	full := fs.Bool("full", false, "")
	pos, err := parseArgs(fs, args, 1)
	if err != nil {
		return err
	}
	if c.actor != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, actorHeader, c.actor)
	}
	stream, err := c.dist.Watch(ctx, &cpv1.WatchRequest{Namespace: pos[0], KnownRevision: *known, ClientId: "cpctl/" + c.actor})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		snap := resp.GetSnapshot()
		fmt.Fprintf(c.out, "%s  %s revision=%d configs=%d flags=%d experiments=%d rate_limits=%d circuit_breakers=%d since_change=%s\n",
			time.Now().Format("15:04:05.000"), snap.GetNamespace(), snap.GetRevision(),
			len(snap.GetConfigs()), len(snap.GetFlags()), len(snap.GetExperiments()),
			len(snap.GetRateLimits()), len(snap.GetCircuitBreakers()),
			time.Since(snap.GetUpdatedAt().AsTime()).Round(100*time.Microsecond))
		if *full {
			if err := c.print(snap); err != nil {
				return err
			}
		}
	}
}

// parseArgs parses flags that may appear before, between or after the
// positional arguments and checks the positional count.
func parseArgs(fs *flag.FlagSet, args []string, want int) ([]string, error) {
	return parseArgsRange(fs, args, want, want)
}

// parseArgsRange is parseArgs for commands with optional positional
// arguments: it accepts from lo to hi of them.
func parseArgsRange(fs *flag.FlagSet, args []string, lo, hi int) ([]string, error) {
	var pos []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil, errHelp
			}
			if strings.Contains(err.Error(), "not defined") {
				// Negative numbers and the like look like flags.
				return nil, fmt.Errorf("%s: %w (put -- before a value that starts with a dash); %w", fs.Name(), err, errUsage)
			}
			return nil, fmt.Errorf("%s: %w; %w", fs.Name(), err, errUsage)
		}
		rest := fs.Args()
		if consumed := len(args) - len(rest); consumed > 0 && args[consumed-1] == "--" {
			pos = append(pos, rest...)
			break
		}
		if len(rest) == 0 {
			break
		}
		pos = append(pos, rest[0])
		args = rest[1:]
	}
	switch {
	case len(pos) >= lo && len(pos) <= hi:
		return pos, nil
	case lo == hi:
		return nil, fmt.Errorf("%s: expected %d argument(s), got %d; %w", fs.Name(), lo, len(pos), errUsage)
	default:
		return nil, fmt.Errorf("%s: expected %d to %d arguments, got %d; %w", fs.Name(), lo, hi, len(pos), errUsage)
	}
}

// parseValue reads s as JSON, falling back to a plain string.
func parseValue(s string) (*structpb.Value, error) {
	v := &structpb.Value{}
	if err := protojson.Unmarshal([]byte(s), v); err == nil {
		return v, nil
	}
	return structpb.NewStringValue(s), nil
}

// parseVariants reads "a=50,b=50" and attaches payloads by variant name.
func parseVariants(spec string, payloads keyValues) ([]*cpv1.Variant, error) {
	if spec == "" {
		return nil, fmt.Errorf("--variants is required, e.g. --variants control=50,treatment=50")
	}
	var out []*cpv1.Variant
	seen := make(map[string]bool)
	for _, part := range strings.Split(spec, ",") {
		name, weight, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || name == "" {
			return nil, fmt.Errorf("--variants: %q is not name=weight", part)
		}
		w, err := strconv.ParseUint(weight, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("--variants: weight of %q: %w", name, err)
		}
		v := &cpv1.Variant{Name: name, Weight: uint32(w)}
		if raw, ok := payloads[name]; ok {
			if v.Payload, err = parseValue(raw); err != nil {
				return nil, err
			}
		}
		seen[name] = true
		out = append(out, v)
	}
	for name := range payloads {
		if !seen[name] {
			return nil, fmt.Errorf("--payload for unknown variant %q", name)
		}
	}
	return out, nil
}

// parsePercent reads a percentage such as "25", "0.5" or "25%". The range is
// the server's to check, with its message, but NaN and infinities are
// refused here: they only get through strconv by spelling.
func parsePercent(s string) (float64, error) {
	return parseFinite(strings.TrimSuffix(strings.TrimSpace(s), "%"))
}

// parseFinite is strconv.ParseFloat without NaN and infinities.
func parseFinite(s string) (float64, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err == nil && (math.IsNaN(f) || math.IsInf(f, 0)) {
		err = fmt.Errorf("%q is not a finite number", s)
	}
	return f, err
}

// floatFlag defines a flag for a float64 field that must be finite.
func floatFlag(fs *flag.FlagSet, name string) *float64 {
	v := new(float64)
	fs.Func(name, "", func(s string) error {
		f, err := parseFinite(s)
		if err != nil {
			return err
		}
		*v = f
		return nil
	})
	return v
}

// table writes aligned columns separated by tabs, like text/tabwriter, but
// without the padding tabwriter leaves behind empty last cells, so that no
// line ends in spaces.
type table struct {
	tw  *tabwriter.Writer
	buf bytes.Buffer
	out io.Writer
}

func newTable(out io.Writer) *table {
	t := &table{out: out}
	t.tw = tabwriter.NewWriter(&t.buf, 0, 4, 2, ' ', 0)
	return t
}

func (t *table) Write(p []byte) (int, error) { return t.tw.Write(p) }

// Flush lays out everything written so far.
func (t *table) Flush() error {
	if err := t.tw.Flush(); err != nil {
		return err
	}
	lines := strings.Split(t.buf.String(), "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(l, " ")
	}
	t.buf.Reset()
	_, err := io.WriteString(t.out, strings.Join(lines, "\n"))
	return err
}

// clean replaces control characters, such as the newlines and escape
// sequences a hostile actor name could carry, so that free text from the
// server cannot break the layout of a table or drive the terminal.
func clean(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return '?'
		}
		return r
	}, s)
}

// formatPercent prints a percentage with at most two decimals, the
// resolution of rollout buckets, which also hides float noise such as
// 0.30000000000000004.
func formatPercent(p float64) string {
	return strconv.FormatFloat(math.Round(p*100)/100, 'f', -1, 64)
}

// formatTime prints a timestamp in local time, or "-" when it is unset.
func formatTime(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return "-"
	}
	return ts.AsTime().Local().Format(time.DateTime)
}

// keyValues is a repeatable name=value flag.
type keyValues map[string]string

func (kv keyValues) String() string { return "" }

func (kv keyValues) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return fmt.Errorf("%q is not name=value", s)
	}
	kv[k] = v
	return nil
}

// stringList is a repeatable flag of comma-separated values. Blanks and
// repeats are dropped, so "--allow a,b --allow a" is [a b].
type stringList []string

func (l *stringList) String() string { return strings.Join(*l, ",") }

func (l *stringList) Set(s string) error {
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" && !slices.Contains(*l, v) {
			*l = append(*l, v)
		}
	}
	return nil
}

// uint32Flag defines a flag for a uint32 field, rejecting values that would
// not fit instead of truncating them.
func uint32Flag(fs *flag.FlagSet, name string) *uint32 {
	v := new(uint32)
	fs.Func(name, "", func(s string) error {
		n, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return err
		}
		*v = uint32(n)
		return nil
	})
	return v
}

// int32Flag is uint32Flag for int32 fields.
func int32Flag(fs *flag.FlagSet, name string) *int32 {
	v := new(int32)
	fs.Func(name, "", func(s string) error {
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		*v = int32(n)
		return nil
	})
	return v
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
