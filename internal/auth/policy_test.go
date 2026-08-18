package auth

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

// caller is a registered test identity.
type caller struct {
	token     string
	principal Principal
}

// newTestAuthenticator registers one caller per role.
func newTestAuthenticator(t *testing.T) (*Authenticator, map[Role]caller) {
	t.Helper()
	names := map[Role]string{RoleReader: "dashboard", RoleEditor: "deploy-bot", RoleAdmin: "ops"}
	callers := make(map[Role]caller, len(names))
	var entries []Entry
	for role, name := range names {
		c := caller{token: mustToken(t), principal: Principal{Name: name, Role: role}}
		callers[role] = c
		entries = append(entries, Entry{Name: name, Role: role.String(), TokenSHA256: HashToken(c.token)})
	}
	return mustNew(t, entries...), callers
}

// TestEveryRPCHasAnExplicitRole fails when an RPC is added to the API
// without deciding who may call it.
func TestEveryRPCHasAnExplicitRole(t *testing.T) {
	readOnlyAdmin := map[string]bool{
		"GetNamespace": true, "ListNamespaces": true, "ListRevisions": true,
		"GetRevision": true, "DiffRevisions": true, "ListAuditEvents": true,
	}
	want := func(sd *grpc.ServiceDesc, method string) Role {
		switch {
		case sd == &cpv1.DistributionService_ServiceDesc, readOnlyAdmin[method]:
			return RoleReader
		case method == "CreateNamespace":
			return RoleAdmin
		default:
			return RoleEditor
		}
	}

	rpcs := 0
	for _, sd := range []*grpc.ServiceDesc{&cpv1.AdminService_ServiceDesc, &cpv1.DistributionService_ServiceDesc} {
		var methods []string
		for _, m := range sd.Methods {
			methods = append(methods, m.MethodName)
		}
		for _, s := range sd.Streams {
			methods = append(methods, s.StreamName)
		}
		for _, m := range methods {
			rpcs++
			full := "/" + sd.ServiceName + "/" + m
			if _, ok := methodRoles[full]; !ok {
				t.Errorf("%s has no explicit entry in methodRoles", full)
				continue
			}
			if role, public := RequiredRole(full); public || role != want(sd, m) {
				t.Errorf("RequiredRole(%s) = %v, public %v; want %v", full, role, public, want(sd, m))
			}
		}
	}
	if len(methodRoles) != rpcs {
		t.Errorf("methodRoles has %d entries for %d RPCs; every entry must be an RPC of the two services", len(methodRoles), rpcs)
	}
}

func TestRequiredRoleOutsideTheAPI(t *testing.T) {
	for _, tc := range []struct {
		method string
		role   Role
		public bool
	}{
		{healthpb.Health_Check_FullMethodName, 0, true},
		{healthpb.Health_Watch_FullMethodName, 0, true},
		{healthpb.Health_List_FullMethodName, 0, true},
		{reflectionv1.ServerReflection_ServerReflectionInfo_FullMethodName, 0, true},
		{reflectionv1alpha.ServerReflection_ServerReflectionInfo_FullMethodName, 0, true},
		// Only the health service itself is public, not look-alikes.
		{"/grpc.health.v1.HealthAdmin/SetServingStatus", RoleAdmin, false},
		// Unknown methods, including future ones of known services, need admin.
		{"/controlplane.v1.AdminService/DropAllNamespaces", RoleAdmin, false},
		{"/controlplane.v1.DistributionService/WatchAll", RoleAdmin, false},
		{"/other.v1.Service/Method", RoleAdmin, false},
		{"", RoleAdmin, false},
	} {
		if role, public := RequiredRole(tc.method); role != tc.role || public != tc.public {
			t.Errorf("RequiredRole(%q) = %v, %v; want %v, %v", tc.method, role, public, tc.role, tc.public)
		}
	}
}

type ctxMarker struct{}

func TestAuthorize(t *testing.T) {
	a, callers := newTestAuthenticator(t)
	reader, editor, admin := callers[RoleReader], callers[RoleEditor], callers[RoleAdmin]
	ctx := context.WithValue(context.Background(), ctxMarker{}, "request")

	for _, tc := range []struct {
		name    string
		method  string
		caller  caller // zero: no principal expected
		token   string
		want    codes.Code
		wantMsg string
	}{
		{name: "public method without a token", method: healthpb.Health_Check_FullMethodName},
		{name: "public method ignores a bad token", method: healthpb.Health_Check_FullMethodName, token: "cp_bogus"},
		{name: "public method ignores a good token", method: healthpb.Health_Watch_FullMethodName, token: reader.token},
		{name: "missing token", method: cpv1.DistributionService_Watch_FullMethodName, want: codes.Unauthenticated, wantMsg: "missing bearer token"},
		{name: "unknown token", method: cpv1.DistributionService_Watch_FullMethodName, token: "cp_bogus", want: codes.Unauthenticated, wantMsg: "invalid bearer token"},
		{name: "reader watches", method: cpv1.DistributionService_Watch_FullMethodName, token: reader.token, caller: reader},
		{name: "reader reads history", method: cpv1.AdminService_DiffRevisions_FullMethodName, token: reader.token, caller: reader},
		{
			name: "reader cannot write", method: cpv1.AdminService_PutFlag_FullMethodName, token: reader.token,
			want: codes.PermissionDenied, wantMsg: `reader "dashboard" may not call /controlplane.v1.AdminService/PutFlag, which requires editor`,
		},
		{name: "reader cannot roll back", method: cpv1.AdminService_Rollback_FullMethodName, token: reader.token, want: codes.PermissionDenied},
		{name: "editor writes", method: cpv1.AdminService_PutFlag_FullMethodName, token: editor.token, caller: editor},
		{name: "editor rolls back", method: cpv1.AdminService_Rollback_FullMethodName, token: editor.token, caller: editor},
		{name: "editor reads", method: cpv1.AdminService_ListAuditEvents_FullMethodName, token: editor.token, caller: editor},
		{name: "editor cannot create namespaces", method: cpv1.AdminService_CreateNamespace_FullMethodName, token: editor.token, want: codes.PermissionDenied, wantMsg: "requires admin"},
		{name: "admin creates namespaces", method: cpv1.AdminService_CreateNamespace_FullMethodName, token: admin.token, caller: admin},
		{name: "unknown method needs admin", method: "/other.v1.Service/Method", token: editor.token, want: codes.PermissionDenied},
		{name: "admin calls unknown method", method: "/other.v1.Service/Method", token: admin.token, caller: admin},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := a.Authorize(ctx, tc.method, tc.token)
			if code := status.Code(err); code != tc.want {
				t.Fatalf("Authorize = %v, want code %v", err, tc.want)
			}
			if err != nil {
				if !strings.Contains(status.Convert(err).Message(), tc.wantMsg) {
					t.Errorf("message %q does not contain %q", status.Convert(err).Message(), tc.wantMsg)
				}
				if got != nil {
					t.Errorf("Authorize returned a context with error %v", err)
				}
				return
			}
			if got.Value(ctxMarker{}) != "request" {
				t.Fatal("Authorize dropped the request context")
			}
			p, ok := PrincipalFrom(got)
			if tc.caller == (caller{}) {
				if got != ctx || ok {
					t.Fatalf("public method: context changed (principal %+v, %v)", p, ok)
				}
				return
			}
			if !ok || p != tc.caller.principal {
				t.Fatalf("principal = %+v, %v; want %+v", p, ok, tc.caller.principal)
			}
		})
	}
}
