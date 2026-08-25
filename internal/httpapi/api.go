package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
)

const (
	apiPrefix = "/api/v1/"
	// maxBodyBytes bounds a request body. The largest legitimate request, a
	// config value, is capped at 256 KiB by the model.
	maxBodyBytes = 1 << 20

	actorHeader = "X-Controlplane-Actor"
	// actorMetadataKey is where the gRPC server looks for the actor
	// (server.ActorHeader).
	actorMetadataKey = "x-controlplane-actor"
)

var (
	// Unknown fields are rejected so that a misspelt field fails loudly
	// instead of being ignored.
	unmarshalOptions = protojson.UnmarshalOptions{}
	marshalOptions   = protojson.MarshalOptions{EmitUnpopulated: true}
)

// method is a unary RPC served at apiPrefix + "<Service>/<Method>".
type method struct {
	fullMethod string // gRPC name, e.g. "/controlplane.v1.AdminService/PutFlag"
	service    any
	// handler is the generated gRPC handler: it decodes the request with the
	// function it is given and calls the method on service.
	handler grpc.MethodHandler
}

// api serves the JSON API.
type api struct {
	methods map[string]method // keyed by URL path
	auth    *auth.Authenticator
	log     *slog.Logger
}

func newAPI(opts Options) *api {
	a := &api{methods: make(map[string]method), auth: opts.Auth, log: opts.Logger}
	if a.log == nil {
		a.log = slog.Default()
	}
	// A nil service must not reach the generated handlers: their type
	// assertion would panic.
	if opts.Admin != nil {
		a.register(&cpv1.AdminService_ServiceDesc, opts.Admin)
	}
	if opts.Distribution != nil {
		a.register(&cpv1.DistributionService_ServiceDesc, opts.Distribution)
	}
	return a
}

// register exposes every unary method of a service. Deriving the dispatch
// table from the generated descriptor means a new RPC is served as soon as
// it is generated. Streams such as Watch have no JSON mapping and stay
// gRPC-only.
func (a *api) register(desc *grpc.ServiceDesc, service any) {
	name := desc.ServiceName[strings.LastIndexByte(desc.ServiceName, '.')+1:]
	for _, m := range desc.Methods {
		a.methods[apiPrefix+name+"/"+m.MethodName] = method{
			fullMethod: "/" + desc.ServiceName + "/" + m.MethodName,
			service:    service,
			handler:    m.Handler,
		}
	}
}

// callError is a failed call: the status reported to the caller and the
// HTTP status it is sent with.
type callError struct {
	httpStatus int
	status     *status.Status
}

func newCallError(httpStatus int, code codes.Code, format string, args ...any) *callError {
	return &callError{httpStatus: httpStatus, status: status.Newf(code, format, args...)}
}

func (a *api) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	m, body, cerr := a.call(w, r)
	httpStatus, code := http.StatusOK, codes.OK
	if cerr != nil {
		httpStatus, code = cerr.httpStatus, cerr.status.Code()
		writeError(w, cerr)
	} else {
		writeJSON(w, http.StatusOK, body)
	}

	level := slog.LevelDebug
	if httpStatus >= http.StatusInternalServerError {
		level = slog.LevelWarn
	}
	a.log.Log(r.Context(), level, "http api call",
		"path", r.URL.Path, "method", m.fullMethod, "status", httpStatus, "code", code.String(),
		"duration", time.Since(start))
}

// call runs the RPC a request names and returns its encoded response.
// Authorization comes before the body is read, so an unauthenticated caller
// can neither make the server buffer a body nor probe request validation.
func (a *api) call(w http.ResponseWriter, r *http.Request) (method, []byte, *callError) {
	m, ok := a.methods[r.URL.Path]
	if !ok {
		return m, nil, newCallError(http.StatusNotFound, codes.NotFound, "unknown method %s", r.URL.Path)
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		return m, nil, newCallError(http.StatusMethodNotAllowed, codes.Unimplemented, "%s needs POST", r.URL.Path)
	}
	ctx, err := a.authorize(r, m.fullMethod)
	if err != nil {
		return m, nil, serviceError(err)
	}
	// Requiring a JSON body is also what keeps other sites out when auth is
	// off: a cross-site form or simple request cannot send this type, and
	// the CORS preflight a page needs for it is never answered.
	if !isJSON(r.Header.Get("Content-Type")) {
		return m, nil, newCallError(http.StatusUnsupportedMediaType, codes.InvalidArgument, "Content-Type must be application/json")
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		if errors.As(err, new(*http.MaxBytesError)) {
			return m, nil, newCallError(http.StatusRequestEntityTooLarge, codes.ResourceExhausted, "request body is larger than %d bytes", maxBodyBytes)
		}
		return m, nil, newCallError(http.StatusBadRequest, codes.InvalidArgument, "read request body: %v", err)
	}

	resp, err := m.handler(m.service, ctx, decoder(body), nil)
	if err != nil {
		return m, nil, serviceError(err)
	}
	msg, ok := resp.(proto.Message)
	if !ok {
		return m, nil, newCallError(http.StatusInternalServerError, codes.Internal, "%s returned %T, not a message", m.fullMethod, resp)
	}
	out, err := marshalOptions.Marshal(msg)
	if err != nil {
		return m, nil, newCallError(http.StatusInternalServerError, codes.Internal, "encode response: %v", err)
	}
	return m, out, nil
}

// authorize returns the context the call runs with: with auth enabled it
// carries the caller's principal; without, the actor named by the request,
// in the incoming metadata where the gRPC server looks for it.
func (a *api) authorize(r *http.Request, fullMethod string) (context.Context, error) {
	ctx := r.Context()
	if a.auth == nil {
		if actor := r.Header.Get(actorHeader); actor != "" {
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(actorMetadataKey, actor))
		}
		return ctx, nil
	}
	return a.auth.Authorize(ctx, fullMethod, bearerToken(r))
}

// bearerToken returns the request's bearer token. Like the gRPC interceptor,
// it treats several Authorization headers as none: which one was meant is
// ambiguous.
func bearerToken(r *http.Request) string {
	vals := r.Header.Values("Authorization")
	if len(vals) != 1 {
		return ""
	}
	return auth.BearerToken(vals[0])
}

// isJSON reports whether a Content-Type header names JSON in UTF-8, the
// only encoding protojson reads.
func isJSON(contentType string) bool {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != "application/json" {
		return false
	}
	charset, ok := params["charset"]
	return !ok || strings.EqualFold(charset, "utf-8")
}

// decoder returns the function the generated handler decodes its request
// message with. An empty body is an empty message, so methods without
// parameters can be called without one.
func decoder(body []byte) func(any) error {
	return func(v any) error {
		msg, ok := v.(proto.Message)
		if !ok {
			return status.Errorf(codes.Internal, "cannot decode a request into %T", v)
		}
		if len(bytes.TrimSpace(body)) == 0 {
			return nil
		}
		if err := unmarshalOptions.Unmarshal(body, msg); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid request body: %v", err)
		}
		return nil
	}
}

// serviceError converts an error from a service or from Authorize. Like
// gRPC, it reports context errors by their own codes and any other error
// that is not a status as Unknown.
func serviceError(err error) *callError {
	st, ok := status.FromError(err)
	if !ok {
		st = status.FromContextError(err)
	}
	return &callError{httpStatus: httpStatusFromCode(st.Code()), status: st}
}

// httpStatusFromCode maps gRPC codes to the HTTP statuses API clients branch
// on; anything else is a server-side failure.
func httpStatusFromCode(code codes.Code) int {
	switch code {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// errorBody is the JSON body of a failed call.
type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, cerr *callError) {
	if cerr.httpStatus == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="controlplane"`)
	}
	// Encoding two strings cannot fail.
	body, _ := json.Marshal(errorBody{Code: cerr.status.Code().String(), Message: cerr.status.Message()})
	writeJSON(w, cerr.httpStatus, body)
}

// writeJSON sends a response body. Responses carry live configuration and
// depend on the caller's token, so no cache may keep them.
func writeJSON(w http.ResponseWriter, httpStatus int, body []byte) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(httpStatus)
	_, _ = w.Write(body)
}
