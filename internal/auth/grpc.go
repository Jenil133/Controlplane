package auth

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// authorizationKey is the metadata key that carries "Bearer <token>".
const authorizationKey = "authorization"

// UnaryServerInterceptor authorizes every unary call with Authorize, taking
// the token from the "authorization: Bearer <token>" metadata.
func (a *Authenticator) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := a.Authorize(ctx, info.FullMethod, incomingToken(ctx))
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor is the streaming counterpart of
// UnaryServerInterceptor. Handlers find the principal in stream.Context().
func (a *Authenticator) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := a.Authorize(ss.Context(), info.FullMethod, incomingToken(ss.Context()))
		if err != nil {
			return err
		}
		return handler(srv, &authorizedStream{ServerStream: ss, ctx: ctx})
	}
}

// authorizedStream hands the authorized context to stream handlers.
type authorizedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authorizedStream) Context() context.Context { return s.ctx }

// incomingToken returns the bearer token of an incoming call. Several
// authorization values count as none: which one was meant is ambiguous, so
// none is trusted.
func incomingToken(ctx context.Context) string {
	vals := metadata.ValueFromIncomingContext(ctx, authorizationKey)
	if len(vals) != 1 {
		return ""
	}
	return BearerToken(vals[0])
}

// BearerToken returns the token of an authorization value "Bearer <token>"
// (RFC 6750; the scheme is case-insensitive and surrounding whitespace is
// ignored), or "" for anything else.
func BearerToken(header string) string {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	// Several spaces may separate scheme and token; a token never contains one.
	token = strings.TrimLeft(token, " ")
	if strings.ContainsAny(token, " \t") {
		return ""
	}
	return token
}

// TokenCredentials returns per-RPC credentials that send
// "authorization: Bearer <token>" with every call. With requireTLS they are
// only ever sent over a connection with privacy and integrity: gRPC refuses
// to pair them with insecure transport credentials. Without it they also work
// in plaintext, e.g. against a local or in-cluster server. An empty token
// sends no header.
func TokenCredentials(token string, requireTLS bool) credentials.PerRPCCredentials {
	return tokenCredentials{token: token, requireTLS: requireTLS}
}

type tokenCredentials struct {
	token      string
	requireTLS bool
}

func (c tokenCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	if c.requireTLS {
		// gRPC enforces RequireTransportSecurity on its own connections; like
		// its oauth credentials, check again so that no other caller can get
		// the token without a secure connection either.
		ri, _ := credentials.RequestInfoFromContext(ctx)
		if err := credentials.CheckSecurityLevel(ri.AuthInfo, credentials.PrivacyAndIntegrity); err != nil {
			return nil, fmt.Errorf("auth: refusing to send token without transport security: %w", err)
		}
	}
	if c.token == "" {
		return nil, nil
	}
	return map[string]string{authorizationKey: "Bearer " + c.token}, nil
}

func (c tokenCredentials) RequireTransportSecurity() bool { return c.requireTLS }
