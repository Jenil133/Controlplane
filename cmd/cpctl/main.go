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
	"os"
	"os/signal"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

// actorHeader must match server.ActorHeader.
const actorHeader = "x-controlplane-actor"

const usage = `cpctl - command-line client for the control plane

Usage:
  cpctl [global flags] <command> [arguments]

Commands:
  ns create <name> [--description text]
  ns get <name>
  ns list
  config put <namespace> <key> <value> [--description text]
  config delete <namespace> <key>
  flag put <namespace> <key> [--enabled] [--description text]
  flag delete <namespace> <key>
  experiment put <namespace> <key> --variants a=50,b=50 [--payload b=<json>]... [--enabled] [--salt s] [--description text]
  experiment delete <namespace> <key>
  snapshot <namespace>
  watch <namespace> [--known-revision n] [--full]

Config values and payloads are parsed as JSON; anything that is not valid JSON
is sent as a string. Use -- before a value that starts with a dash.

Global flags:
  --addr string      control plane gRPC address (env CPCTL_ADDR, default localhost:9090)
  --actor string     name recorded on changes (env CPCTL_ACTOR, default $USER)
  --timeout duration per-request timeout (default 10s)
`

var errUsage = errors.New("see 'cpctl help'")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		if s, ok := status.FromError(err); ok && s.Code() != 0 {
			fmt.Fprintf(os.Stderr, "cpctl: %s: %s\n", s.Code(), s.Message())
		} else {
			fmt.Fprintln(os.Stderr, "cpctl:", err)
		}
		os.Exit(1)
	}
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
	actor := global.String("actor", envOr("CPCTL_ACTOR", os.Getenv("USER")), "")
	timeout := global.Duration("timeout", 10*time.Second, "")
	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(out, usage)
			return nil
		}
		return err
	}
	args = global.Args()
	if len(args) == 0 || args[0] == "help" {
		fmt.Fprint(out, usage)
		return nil
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := &cli{
		admin:   cpv1.NewAdminServiceClient(conn),
		dist:    cpv1.NewDistributionServiceClient(conn),
		actor:   *actor,
		timeout: *timeout,
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
	case "snapshot":
		return c.snapshot(ctx, rest)
	case "watch":
		return c.watch(ctx, rest)
	default:
		return fmt.Errorf("unknown command %q; %w", cmd, errUsage)
	}
}

func (c *cli) call(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
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
		tw := tabwriter.NewWriter(c.out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tREVISION\tUPDATED\tDESCRIPTION")
		for _, ns := range resp.GetNamespaces() {
			fmt.Fprintf(tw, "%s\t%d\t%s\t%s\n", ns.GetName(), ns.GetRevision(),
				ns.GetUpdatedAt().AsTime().Local().Format(time.DateTime), ns.GetDescription())
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
			Namespace: pos[0], Key: pos[1], Value: value, Description: *description,
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
		resp, err := c.admin.DeleteConfig(ctx, &cpv1.DeleteConfigRequest{Namespace: pos[0], Key: pos[1]})
		if err != nil {
			return err
		}
		fmt.Fprintf(c.out, "deleted config %s (namespace %s now at revision %d)\n", pos[1], pos[0], resp.GetRevision())
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
	ctx, cancel := c.call(ctx)
	defer cancel()

	switch args[0] {
	case "put":
		pos, err := parseArgs(fs, args[1:], 2)
		if err != nil {
			return err
		}
		resp, err := c.admin.PutFlag(ctx, &cpv1.PutFlagRequest{
			Namespace: pos[0], Key: pos[1], Enabled: *enabled, Description: *description,
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
		resp, err := c.admin.DeleteFlag(ctx, &cpv1.DeleteFlagRequest{Namespace: pos[0], Key: pos[1]})
		if err != nil {
			return err
		}
		fmt.Fprintf(c.out, "deleted flag %s (namespace %s now at revision %d)\n", pos[1], pos[0], resp.GetRevision())
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
		resp, err := c.admin.DeleteExperiment(ctx, &cpv1.DeleteExperimentRequest{Namespace: pos[0], Key: pos[1]})
		if err != nil {
			return err
		}
		fmt.Fprintf(c.out, "deleted experiment %s (namespace %s now at revision %d)\n", pos[1], pos[0], resp.GetRevision())
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
		fmt.Fprintf(c.out, "%s  %s revision=%d configs=%d flags=%d experiments=%d since_change=%s\n",
			time.Now().Format("15:04:05.000"), snap.GetNamespace(), snap.GetRevision(),
			len(snap.GetConfigs()), len(snap.GetFlags()), len(snap.GetExperiments()),
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
	var pos []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, fmt.Errorf("%s: %w", fs.Name(), err)
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
	if len(pos) != want {
		return nil, fmt.Errorf("%s: expected %d argument(s), got %d; %w", fs.Name(), want, len(pos), errUsage)
	}
	return pos, nil
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

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
