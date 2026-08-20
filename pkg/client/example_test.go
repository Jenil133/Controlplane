package client_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/pkg/breaker"
	"github.com/Jenil133/Controlplane/pkg/client"
)

// A checkout service reads flags, an experiment and configs from the control
// plane, and lets it set the rate limit of its endpoint and the circuit
// breaker in front of its payments provider.
func Example() {
	ctx := context.Background()
	cp, err := client.New(ctx, client.Options{
		Address:   "controlplane.internal:9090",
		Namespace: "checkout/prod",
		Token:     os.Getenv("CONTROLPLANE_TOKEN"),
		// With a cache, the service starts on the configuration it last saw
		// even when the control plane is unreachable.
		CachePath: "/var/cache/checkout/controlplane.json",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cp.Close()

	// Without a cache file yet, give the control plane a moment.
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cp.WaitReady(waitCtx); err != nil {
		log.Printf("starting with defaults: %v", err)
	}
	cp.OnChange(func(old, new *cpv1.Snapshot) {
		log.Printf("configuration changed: revision %d -> %d", old.GetRevision(), new.GetRevision())
	})

	// Calls to the payments provider go through the circuit breaker
	// "payments".
	payments := &http.Client{Transport: cp.BreakerTransport(func(*http.Request) string { return "payments" }, nil)}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /checkout", func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		cart := "classic"
		if cp.IsEnabled("new-cart", user) {
			cart = "new"
		}
		button := "blue"
		if a, ok := cp.Variant("cta-color", user); ok {
			button = a.Variant
		}

		ctx, cancel := context.WithTimeout(r.Context(), cp.Duration("payments.timeout", 2*time.Second))
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, cp.String("payments.url", "http://payments:8080/charge"), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp, err := payments.Do(req)
		if errors.Is(err, breaker.ErrOpen) {
			http.Error(w, "payments are unavailable, try again shortly", http.StatusServiceUnavailable)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		resp.Body.Close()
		fmt.Fprintf(w, "paid with the %s cart (%s button)\n", cart, button)
	})

	// POST /checkout is limited by the rate limit "checkout": the URL path
	// without its leading slash.
	log.Fatal(http.ListenAndServe(":8080", cp.RateLimit(nil, mux)))
}

// A gRPC server enforces the namespace's rate limits per method.
func ExampleClient_UnaryServerInterceptor() {
	cp, err := client.New(context.Background(), client.Options{Address: "controlplane.internal:9090", Namespace: "orders/prod"})
	if err != nil {
		log.Fatal(err)
	}
	defer cp.Close()

	// Each method is limited by the rate limit named after it, such as
	// "orders.v1.Orders/Create"; calls over the limit fail with
	// RESOURCE_EXHAUSTED.
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(cp.UnaryServerInterceptor(nil)),
		grpc.ChainStreamInterceptor(cp.StreamServerInterceptor(nil)),
	)
	// Register the services on srv, then serve.
	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(srv.Serve(lis))
}

// Outgoing gRPC calls share a circuit breaker.
func ExampleClient_UnaryClientInterceptor() {
	cp, err := client.New(context.Background(), client.Options{Address: "controlplane.internal:9090", Namespace: "orders/prod"})
	if err != nil {
		log.Fatal(err)
	}
	defer cp.Close()

	// Every call to the inventory service goes through the circuit breaker
	// "inventory". While it is open, calls fail at once with UNAVAILABLE and
	// errors.Is(err, breaker.ErrOpen) holds.
	conn, err := grpc.NewClient("inventory.internal:9090",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(cp.UnaryClientInterceptor(func(string) string { return "inventory" })),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	// Create the inventory service's client from conn as usual.
}

// The cache keeps a service running on the last configuration it saw while
// the control plane is unreachable.
func Example_lastKnownGood() {
	dir, err := os.MkdirTemp("", "controlplane-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	cachePath := filepath.Join(dir, "checkout.json")

	// The cache file as an earlier run left it: the namespace's snapshot as
	// protojson.
	cached, err := protojson.Marshal(&cpv1.Snapshot{
		Namespace: "checkout/prod",
		Revision:  42,
		Configs:   []*cpv1.Config{{Key: "greeting", Value: structpb.NewStringValue("hello")}},
		Flags:     []*cpv1.Flag{{Key: "new-cart", Enabled: true, RolloutPercent: 100}},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(cachePath, cached, 0o600); err != nil {
		log.Fatal(err)
	}

	cp, err := client.New(context.Background(), client.Options{
		Address:   "127.0.0.1:1", // nothing listens here: the control plane is down
		Namespace: "checkout/prod",
		CachePath: cachePath,
		Logger:    slog.New(slog.DiscardHandler), // keep the reconnect warnings out of the output
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cp.Close()

	if err := cp.WaitReady(context.Background()); err != nil {
		log.Fatal(err)
	}
	st := cp.Status()
	fmt.Println("serving revision", st.Revision, "from the", st.Source)
	fmt.Println("greeting:", cp.String("greeting", "hi"))
	fmt.Println("new-cart for alice:", cp.IsEnabled("new-cart", "alice"))
	// Output:
	// serving revision 42 from the cache
	// greeting: hello
	// new-cart for alice: true
}
