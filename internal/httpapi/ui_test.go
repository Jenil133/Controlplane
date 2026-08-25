package httpapi

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/auth"
)

func readUIFile(t *testing.T, name string) string {
	t.Helper()
	b, err := uiFS.ReadFile("ui/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func get(h http.Handler, method, path string, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestSecurityHeaders(t *testing.T) {
	f := newFixture(t, nil)
	for _, req := range []*http.Request{
		apiRequest("/api/v1/AdminService/ListNamespaces", "{}"),
		apiRequest("/api/v1/AdminService/NoSuchMethod", "{}"),
		httptest.NewRequest(http.MethodGet, "/ui/", nil),
		httptest.NewRequest(http.MethodGet, "/ui/app.js", nil),
		httptest.NewRequest(http.MethodGet, "/ui/missing.js", nil),
		httptest.NewRequest(http.MethodGet, "/", nil),
		httptest.NewRequest(http.MethodDelete, "/", nil),
		httptest.NewRequest(http.MethodGet, "/elsewhere", nil),
	} {
		w := f.serve(req)
		for name, want := range map[string]string{
			"Content-Security-Policy": "default-src 'self'",
			"X-Content-Type-Options":  "nosniff",
			"X-Frame-Options":         "DENY",
			"Referrer-Policy":         "no-referrer",
		} {
			if got := w.Header().Get(name); got != want {
				t.Errorf("%s %s (status %d): %s = %q, want %q", req.Method, req.URL.Path, w.Code, name, got, want)
			}
		}
	}
}

// TestSecurityHeadersOnRejections covers the responses that come from
// somewhere other than a successful handler: authentication failures, bad
// requests, mux redirects, 304s and 405s.
func TestSecurityHeadersOnRejections(t *testing.T) {
	f := newFixture(t, newTestAuthenticator(t))
	notModified := httptest.NewRequest(http.MethodGet, "/ui/app.js", nil)
	notModified.Header.Set("If-None-Match", f.serve(httptest.NewRequest(http.MethodGet, "/ui/app.js", nil)).Header().Get("ETag"))
	badType := apiRequest("/api/v1/AdminService/ListNamespaces", "{}", "Authorization", bearer(auth.RoleAdmin))
	badType.Header.Set("Content-Type", "text/plain")
	for what, req := range map[string]*http.Request{
		"401":                  apiRequest("/api/v1/AdminService/ListNamespaces", "{}"),
		"403":                  apiRequest("/api/v1/AdminService/PutFlag", "{}", "Authorization", bearer(auth.RoleReader)),
		"415":                  badType,
		"400":                  apiRequest("/api/v1/AdminService/ListNamespaces", "{", "Authorization", bearer(auth.RoleAdmin)),
		"405":                  httptest.NewRequest(http.MethodGet, "/api/v1/AdminService/ListNamespaces", nil),
		"304":                  notModified,
		"redirect /ui":         httptest.NewRequest(http.MethodGet, "/ui", nil),
		"redirect unclean api": apiRequest("/api/v1/../v1/AdminService/ListNamespaces", "{}"),
		"redirect unclean ui":  httptest.NewRequest(http.MethodGet, "/ui//app.js", nil),
		"405 on /ui/":          httptest.NewRequest(http.MethodPost, "/ui/", nil),
		"404 under /api/":      httptest.NewRequest(http.MethodGet, "/api/nothing", nil),
	} {
		w := f.serve(req)
		for name, want := range map[string]string{
			"Content-Security-Policy": "default-src 'self'",
			"X-Content-Type-Options":  "nosniff",
			"X-Frame-Options":         "DENY",
			"Referrer-Policy":         "no-referrer",
		} {
			if got := w.Header().Get(name); got != want {
				t.Errorf("%s response (status %d): %s = %q, want %q", what, w.Code, name, got, want)
			}
		}
	}
}

// TestUIServesOnlyItsFiles tries to reach other files than the embedded UI
// through path tricks. The handler looks files up by exact URL path in a
// table, so none of these may produce a file, in particular not the Go
// source next to the UI.
func TestUIServesOnlyItsFiles(t *testing.T) {
	h := New(Options{Logger: slog.New(slog.DiscardHandler)})
	for _, path := range []string{
		"/ui/../ui.go",
		"/ui/../api.go",
		"/ui/%2e%2e/ui.go",
		"/ui/..%2fui.go",
		"/ui/%2e%2e%2fui.go",
		"/ui/..%5cui.go",
		"/ui/../../go.mod",
		"/ui/./../ui.go",
		"/ui/ui/app.js",
		"/ui/app.js/",
		"/ui/app.js/..",
		"/ui/app.js%00.css",
		"/ui/APP.JS",
		"/ui/.",
		"/ui/ui.go",
		"/ui/testdata/uilogic.mjs",
	} {
		w := get(h, http.MethodGet, path)
		if w.Code == http.StatusOK {
			t.Errorf("GET %s: status 200, body %.60q", path, w.Body)
		}
		if strings.Contains(w.Body.String(), "package httpapi") {
			t.Errorf("GET %s leaked Go source", path)
		}
	}
}

func TestUIFiles(t *testing.T) {
	h := New(Options{Logger: slog.New(slog.DiscardHandler)})
	for _, tc := range []struct {
		path, contentType, contains string
	}{
		{"/ui/", "text/html; charset=utf-8", `<script type="module" src="app.js"></script>`},
		{"/ui/index.html", "text/html; charset=utf-8", "<title>Controlplane</title>"},
		{"/ui/app.js", "text/javascript; charset=utf-8", "async function rpc("},
		{"/ui/style.css", "text/css; charset=utf-8", ":root {"},
		{"/ui/favicon.svg", "image/svg+xml", "<svg"},
	} {
		w := get(h, http.MethodGet, tc.path)
		if w.Code != http.StatusOK {
			t.Errorf("GET %s: status %d", tc.path, w.Code)
			continue
		}
		if got := w.Header().Get("Content-Type"); got != tc.contentType {
			t.Errorf("GET %s: Content-Type %q, want %q", tc.path, got, tc.contentType)
		}
		if !strings.Contains(w.Body.String(), tc.contains) {
			t.Errorf("GET %s: body lacks %q", tc.path, tc.contains)
		}
		if got := w.Header().Get("Cache-Control"); got != "no-cache" {
			t.Errorf("GET %s: Cache-Control %q, want no-cache", tc.path, got)
		}
		etag := w.Header().Get("ETag")
		if etag == "" {
			t.Errorf("GET %s: no ETag", tc.path)
			continue
		}
		if w := get(h, http.MethodGet, tc.path, "If-None-Match", etag); w.Code != http.StatusNotModified {
			t.Errorf("GET %s with its ETag: status %d, want 304", tc.path, w.Code)
		}
	}

	if w := get(h, http.MethodHead, "/ui/"); w.Code != http.StatusOK || w.Body.Len() != 0 {
		t.Errorf("HEAD /ui/: status %d, %d body bytes", w.Code, w.Body.Len())
	}
	for _, tc := range []struct {
		method, path string
		status       int // 0: any redirect, whose status the mux picks
		location     string
	}{
		{http.MethodGet, "/", http.StatusFound, "/ui/"},
		{http.MethodGet, "/ui", 0, "/ui/"},
		{http.MethodGet, "/ui/missing.js", http.StatusNotFound, ""},
		{http.MethodGet, "/favicon.ico", http.StatusNotFound, ""},
		{http.MethodPost, "/ui/", http.StatusMethodNotAllowed, ""},
	} {
		w := get(h, tc.method, tc.path)
		statusOK := w.Code == tc.status || tc.status == 0 && w.Code >= 300 && w.Code < 400
		if !statusOK || w.Header().Get("Location") != tc.location {
			t.Errorf("%s %s: status %d, Location %q; want %d, %q", tc.method, tc.path, w.Code, w.Header().Get("Location"), tc.status, tc.location)
		}
	}

	for path, f := range newUI().files {
		if f.contentType == "application/octet-stream" {
			t.Errorf("%s has no content type; add its extension to uiContentTypes", path)
		}
	}
}

// TestUIIsCSPClean guards what the Content-Security-Policy would otherwise
// break at runtime, and the rule that data is never parsed as HTML.
func TestUIIsCSPClean(t *testing.T) {
	html := readUIFile(t, "index.html")
	for _, m := range regexp.MustCompile(`(?is)<script\b([^>]*)>(.*?)</script>`).FindAllStringSubmatch(html, -1) {
		if !strings.Contains(m[1], "src=") || strings.TrimSpace(m[2]) != "" {
			t.Errorf("index.html has an inline script: %s", m[0])
		}
	}
	for _, re := range []string{`(?i)<style`, `(?i)\sstyle\s*=`, `(?i)\son[a-z]+\s*=`, `(?i)javascript:`} {
		if loc := regexp.MustCompile(re).FindStringIndex(html); loc != nil {
			t.Errorf("index.html matches %s: %q", re, html[loc[0]:min(loc[1]+40, len(html))])
		}
	}
	for _, m := range regexp.MustCompile(`(?:src|href)="([^"#][^"]*)"`).FindAllStringSubmatch(html, -1) {
		if _, err := fs.Stat(uiFS, "ui/"+m[1]); err != nil {
			t.Errorf("index.html loads %s, which is not embedded", m[1])
		}
	}

	entries, err := fs.ReadDir(uiFS, "ui")
	if err != nil {
		t.Fatal(err)
	}
	external := regexp.MustCompile(`(?i)https?://|(?:src|href)\s*=\s*["']//|url\(\s*["']?//`)
	for _, e := range entries {
		content := strings.ReplaceAll(readUIFile(t, e.Name()), `xmlns="http://www.w3.org/2000/svg"`, "")
		if loc := external.FindStringIndex(content); loc != nil {
			t.Errorf("%s references an external resource: %q", e.Name(), content[loc[0]:min(loc[1]+40, len(content))])
		}
	}

	js := readUIFile(t, "app.js")
	for _, banned := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(", "new Function", ".style.", "cssText"} {
		if strings.Contains(js, banned) {
			t.Errorf("app.js uses %s", banned)
		}
	}
	if loc := regexp.MustCompile(`\bstyle\s*:|'style'|"style"`).FindStringIndex(js); loc != nil {
		t.Errorf("app.js sets a style attribute: %q", js[loc[0]:loc[1]])
	}
}

var (
	// rpcCallRE matches rpc('/api/v1/<Service>/<Method>', up to the request.
	rpcCallRE    = regexp.MustCompile(`\brpc\(\s*'(/api/v1/(\w+)/(\w+))'\s*,\s*`)
	quotedPathRE = regexp.MustCompile("['\"`]/api/v1/")
)

// TestUICallsMatchTheAPI checks every API call in app.js against the proto
// definitions: the method exists and is served, and every request field is
// spelt as protojson's lowerCamelCase JSON name, recursively for nested
// messages. It also requires the UI to reach every unary RPC.
func TestUICallsMatchTheAPI(t *testing.T) {
	src := readUIFile(t, "app.js")
	table := newAPI(Options{
		Admin:        cpv1.UnimplementedAdminServiceServer{},
		Distribution: cpv1.UnimplementedDistributionServiceServer{},
	}).methods
	services := cpv1.File_controlplane_v1_controlplane_proto.Services()

	calls := rpcCallRE.FindAllStringSubmatchIndex(src, -1)
	called := make(map[string]bool)
	for _, m := range calls {
		path, service, method := src[m[2]:m[3]], src[m[4]:m[5]], src[m[6]:m[7]]
		where := fmt.Sprintf("app.js:%d: %s", 1+strings.Count(src[:m[0]], "\n"), path)
		called[path] = true
		if _, ok := table[path]; !ok {
			t.Errorf("%s: not in the dispatch table", where)
		}
		sd := services.ByName(protoreflect.Name(service))
		if sd == nil {
			t.Errorf("%s: no service %s in the proto", where, service)
			continue
		}
		md := sd.Methods().ByName(protoreflect.Name(method))
		if md == nil {
			t.Errorf("%s: %s has no method %s", where, service, method)
			continue
		}
		if md.IsStreamingClient() || md.IsStreamingServer() {
			t.Errorf("%s: streaming methods are not served over HTTP", where)
		}
		if src[m[1]] != '{' {
			t.Errorf("%s: the request must be an object literal so that its fields can be checked", where)
			continue
		}
		literal, _, err := jsLiteral(src, m[1])
		if err != nil {
			t.Errorf("%s: %v", where, err)
			continue
		}
		for _, problem := range checkFields(literal, md.Input()) {
			t.Errorf("%s: %s", where, problem)
		}
	}

	// Calls the patterns above cannot see would escape the checks.
	if n := len(quotedPathRE.FindAllString(src, -1)); n != len(calls) {
		t.Errorf("app.js has %d API paths but %d checkable rpc('...', {...}) calls", n, len(calls))
	}
	uses := len(regexp.MustCompile(`\brpc\(`).FindAllString(src, -1)) - len(regexp.MustCompile(`\bfunction rpc\(`).FindAllString(src, -1))
	if uses != len(calls) {
		t.Errorf("app.js calls rpc %d times, %d of them with a literal path", uses, len(calls))
	}
	for path := range table {
		if !called[path] {
			t.Errorf("the UI never calls %s", path)
		}
	}
}

// TestRequestLiteralChecker makes sure the checker behind
// TestUICallsMatchTheAPI notices mistakes.
func TestRequestLiteralChecker(t *testing.T) {
	methods := cpv1.File_controlplane_v1_controlplane_proto.Services().ByName("AdminService").Methods()
	for _, tc := range []struct {
		method   string
		literal  string
		problems int
	}{
		{"DeleteFlag", `{ namespace, key: item.key, expectedRevision: item.revision }`, 0},
		{"DeleteFlag", `{ namespace, key, expected_revision: 3 }`, 1},
		{"DeleteFlag", `{ ...defaults, key }`, 1},
		{"PutFlag", "{\n  // a comment, with a comma\n  key: 'a,b', description: `x${y ? '{' : '}'}`, /* note, here */ salt: \"c,d\",\n}", 0},
		{"PutExperiment", `{ variants: list.map((v) => ({ name: v.name, weight: v.weight, payload: v.payload })) }`, 0},
		{"PutExperiment", `{ variants: list.map((v) => ({ name: v.name, wieght: v.weight })) }`, 1},
		{"StartRollout", `{ flag: f.key, stages: s.map((x) => ({ percent: x.p, durationNs: x.d })) }`, 1},
		{"PutConfig", `{ key: 'k', value: { anything: { goes: [1, 2] } } }`, 0},
		{"ListAuditEvents", `{ since: new Date(x).toISOString(), page_token: t }`, 1},
	} {
		literal, end, err := jsLiteral(tc.literal+" trailing", 0)
		if err != nil || end != len(tc.literal) {
			t.Errorf("jsLiteral(%q) = end %d, %v; want end %d", tc.literal, end, err, len(tc.literal))
			continue
		}
		problems := checkFields(literal, methods.ByName(protoreflect.Name(tc.method)).Input())
		if len(problems) != tc.problems {
			t.Errorf("%s %s: problems %q, want %d", tc.method, tc.literal, problems, tc.problems)
		}
	}
}

// The helpers below understand just enough JavaScript for the request
// literals in app.js: strings, template literals, comments and brackets.

// jsLiteral returns the bracketed expression that starts at src[start],
// with comments blanked out, and the index just past it.
func jsLiteral(src string, start int) (string, int, error) {
	var b strings.Builder
	end, err := scanBalanced(src, start, &b)
	return b.String(), end, err
}

func scanBalanced(src string, i int, out *strings.Builder) (int, error) {
	closers := map[byte]byte{'(': ')', '[': ']', '{': '}'}
	var open []byte
	for i < len(src) {
		c := src[i]
		switch {
		case strings.HasPrefix(src[i:], "//"):
			end := strings.IndexByte(src[i:], '\n')
			if end < 0 {
				return 0, errors.New("unterminated expression")
			}
			out.WriteByte(' ')
			i += end
			continue
		case strings.HasPrefix(src[i:], "/*"):
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				return 0, errors.New("unterminated comment")
			}
			out.WriteByte(' ')
			i += 2 + end + 2
			continue
		case c == '\'' || c == '"' || c == '`':
			end, err := skipString(src, i)
			if err != nil {
				return 0, err
			}
			out.WriteString(src[i:end])
			i = end
			continue
		case c == '(' || c == '[' || c == '{':
			open = append(open, closers[c])
		case c == ')' || c == ']' || c == '}':
			if len(open) == 0 || open[len(open)-1] != c {
				return 0, fmt.Errorf("unbalanced %q at offset %d", c, i)
			}
			open = open[:len(open)-1]
			if len(open) == 0 {
				out.WriteByte(c)
				return i + 1, nil
			}
		}
		out.WriteByte(c)
		i++
	}
	return 0, errors.New("unterminated expression")
}

// skipString returns the index just past the string or template literal
// that starts at s[i].
func skipString(s string, i int) (int, error) {
	quote := s[i]
	for j := i + 1; j < len(s); j++ {
		switch {
		case s[j] == '\\':
			j++
		case s[j] == quote:
			return j + 1, nil
		case quote == '`' && strings.HasPrefix(s[j:], "${"):
			end, err := scanBalanced(s, j+1, &strings.Builder{})
			if err != nil {
				return 0, err
			}
			j = end - 1
		case s[j] == '\n' && quote != '`':
			return 0, fmt.Errorf("unterminated string at offset %d", i)
		}
	}
	return 0, fmt.Errorf("unterminated string at offset %d", i)
}

// topLevel splits the inside of a bracketed expression at its own commas.
func topLevel(s string) ([]string, error) {
	var parts []string
	depth, last := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'', '"', '`':
			end, err := skipString(s, i)
			if err != nil {
				return nil, err
			}
			i = end - 1
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[last:i])
				last = i + 1
			}
		}
	}
	return append(parts, s[last:]), nil
}

// objectLiterals returns the outermost object literals in an expression,
// such as the one an arrow function passed to map returns.
func objectLiterals(expr string) ([]string, error) {
	var literals []string
	for i := 0; i < len(expr); i++ {
		switch expr[i] {
		case '\'', '"', '`':
			end, err := skipString(expr, i)
			if err != nil {
				return nil, err
			}
			i = end - 1
		case '{':
			literal, end, err := jsLiteral(expr, i)
			if err != nil {
				return nil, err
			}
			literals = append(literals, literal)
			i = end - 1
		}
	}
	return literals, nil
}

var (
	propertyRE  = regexp.MustCompile(`^([A-Za-z_$][\w$]*)\s*:`)
	shorthandRE = regexp.MustCompile(`^[A-Za-z_$][\w$]*$`)
)

// checkFields returns what is wrong with the properties of an object
// literal meant as the protojson encoding of md. Values of message fields
// are checked against their message, except for well-known types such as
// Value, Duration and Timestamp, whose JSON is not an object of fields.
func checkFields(literal string, md protoreflect.MessageDescriptor) []string {
	parts, err := topLevel(literal[1 : len(literal)-1])
	if err != nil {
		return []string{err.Error()}
	}
	var problems []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		var name, value string
		switch m := propertyRE.FindStringSubmatch(part); {
		case part == "":
			continue
		case m != nil:
			name, value = m[1], part[len(m[0]):]
		case shorthandRE.MatchString(part):
			name, value = part, part
		default:
			problems = append(problems, fmt.Sprintf("cannot check property %q of %s", part, md.FullName()))
			continue
		}
		fd := md.Fields().ByJSONName(name)
		if fd == nil {
			problems = append(problems, fmt.Sprintf("%s has no field with JSON name %q", md.FullName(), name))
			continue
		}
		if fd.Message() == nil || strings.HasPrefix(string(fd.Message().FullName()), "google.protobuf.") {
			continue
		}
		literals, err := objectLiterals(value)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		for _, lit := range literals {
			problems = append(problems, checkFields(lit, fd.Message())...)
		}
	}
	return problems
}

// TestUIRequestsCarryTheRightSemantics checks what TestUICallsMatchTheAPI
// cannot: field names are right, but a rollback that forgot its guard, or a
// delete that forgot the revision it saw, would still pass it.
func TestUIRequestsCarryTheRightSemantics(t *testing.T) {
	src := readUIFile(t, "app.js")
	literals := make(map[string]string) // method path -> request literals, joined
	for _, m := range rpcCallRE.FindAllStringSubmatchIndex(src, -1) {
		literal, _, err := jsLiteral(src, m[1])
		if err != nil {
			t.Fatal(err)
		}
		literals[src[m[2]:m[3]]] += literal + "\n"
	}
	has := func(path, field string) bool {
		return regexp.MustCompile(`(?:^|[{,\s])` + field + `\b`).MatchString(literals["/api/v1/AdminService/"+path])
	}

	// A write to an existing entry names the revision the page showed.
	for _, method := range []string{"DeleteConfig", "DeleteFlag", "DeleteExperiment", "DeleteRateLimit", "DeleteCircuitBreaker", "Rollback"} {
		if !has(method, "expectedRevision") {
			t.Errorf("%s does not send expectedRevision", method)
		}
	}
	for method, want := range map[string]int{"PutConfig": 1, "PutFlag": 2, "PutExperiment": 2, "PutRateLimit": 2, "PutCircuitBreaker": 2} {
		// The form sends it for an existing entry; so does the enabled
		// switch of every kind that has one.
		if n := strings.Count(literals["/api/v1/AdminService/"+method], "expectedRevision"); n != want {
			t.Errorf("%s sends expectedRevision in %d requests, want %d", method, n, want)
		}
	}
	if !has("Rollback", "toRevision") {
		t.Error("Rollback does not send toRevision")
	}

	// The rollback is guarded by the revision on screen and by a confirmation
	// that comes before the call.
	body := regexp.MustCompile(`(?s)async function rollback\(.*?\n}\n`).FindString(src)
	if body == "" {
		t.Fatal("app.js has no rollback function")
	}
	confirmAt, callAt := strings.Index(body, "confirm("), strings.Index(body, "rpc('/api/v1/AdminService/Rollback'")
	if confirmAt < 0 || callAt < confirmAt {
		t.Error("rollback must ask for confirmation before calling Rollback")
	}
	if !strings.Contains(body, "const current = num(state.snapshot.revision)") || !strings.Contains(body, "expectedRevision: current") {
		t.Error("rollback must send the revision the page shows as expectedRevision")
	}

	// Every destructive action asks first.
	for _, fn := range []string{"removeEntry", "rolloutStep"} {
		if !strings.Contains(regexp.MustCompile(`(?s)async function `+fn+`\(.*?\n}\n`).FindString(src), "confirm(") {
			t.Errorf("%s never asks for confirmation", fn)
		}
	}

	// The percentage is sent only when it changed: unset keeps the current
	// value, which a rollout in progress insists on.
	if !strings.Contains(src, "rolloutPercent: changed ? value : undefined") {
		t.Error("the flag form must leave rolloutPercent unset unless the user changed it")
	}
	if strings.Contains(literals["/api/v1/AdminService/PutFlag"], "rolloutPercent: item") {
		t.Error("the enabled switch must not send the flag's rolloutPercent back")
	}
}

// TestUILogic runs the UI's pure helpers (duration and stage syntax, int64
// strings, allowlist splitting) under node. The UI has no build step and no
// JavaScript toolchain, so the test is skipped where node is missing.
func TestUILogic(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	script, err := filepath.Abs(filepath.Join("testdata", "uilogic.mjs"))
	if err != nil {
		t.Fatal(err)
	}
	app, err := filepath.Abs(filepath.Join("ui", "app.js"))
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(node, script, app).CombinedOutput(); err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
}
