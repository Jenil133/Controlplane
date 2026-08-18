package auth

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
)

// publicPrefixes match methods that need no token: load balancers and
// kubelets probe health without credentials, and reflection only describes
// the API.
var publicPrefixes = []string{"/grpc.health.v1.Health/", "/grpc.reflection."}

// methodRoles is the role each control plane RPC requires. A method missing
// here requires RoleAdmin, so a new RPC stays locked down until it is
// classified; the tests fail until then.
var methodRoles = map[string]Role{
	cpv1.AdminService_CreateNamespace_FullMethodName: RoleAdmin,

	cpv1.AdminService_GetNamespace_FullMethodName:    RoleReader,
	cpv1.AdminService_ListNamespaces_FullMethodName:  RoleReader,
	cpv1.AdminService_ListRevisions_FullMethodName:   RoleReader,
	cpv1.AdminService_GetRevision_FullMethodName:     RoleReader,
	cpv1.AdminService_DiffRevisions_FullMethodName:   RoleReader,
	cpv1.AdminService_ListAuditEvents_FullMethodName: RoleReader,

	cpv1.AdminService_PutConfig_FullMethodName:            RoleEditor,
	cpv1.AdminService_DeleteConfig_FullMethodName:         RoleEditor,
	cpv1.AdminService_PutFlag_FullMethodName:              RoleEditor,
	cpv1.AdminService_DeleteFlag_FullMethodName:           RoleEditor,
	cpv1.AdminService_PutExperiment_FullMethodName:        RoleEditor,
	cpv1.AdminService_DeleteExperiment_FullMethodName:     RoleEditor,
	cpv1.AdminService_PutRateLimit_FullMethodName:         RoleEditor,
	cpv1.AdminService_DeleteRateLimit_FullMethodName:      RoleEditor,
	cpv1.AdminService_PutCircuitBreaker_FullMethodName:    RoleEditor,
	cpv1.AdminService_DeleteCircuitBreaker_FullMethodName: RoleEditor,
	cpv1.AdminService_StartRollout_FullMethodName:         RoleEditor,
	cpv1.AdminService_AdvanceRollout_FullMethodName:       RoleEditor,
	cpv1.AdminService_PauseRollout_FullMethodName:         RoleEditor,
	cpv1.AdminService_ResumeRollout_FullMethodName:        RoleEditor,
	cpv1.AdminService_AbortRollout_FullMethodName:         RoleEditor,
	cpv1.AdminService_Rollback_FullMethodName:             RoleEditor,

	cpv1.DistributionService_GetSnapshot_FullMethodName: RoleReader,
	cpv1.DistributionService_Watch_FullMethodName:       RoleReader,
}

// RequiredRole returns the role needed to call fullMethod
// ("/package.Service/Method"). Health checks and reflection are public, and
// role is then zero. Every AdminService and DistributionService RPC has an
// explicit role; anything else requires RoleAdmin.
func RequiredRole(fullMethod string) (role Role, public bool) {
	for _, prefix := range publicPrefixes {
		if strings.HasPrefix(fullMethod, prefix) {
			return 0, true
		}
	}
	if r, ok := methodRoles[fullMethod]; ok {
		return r, false
	}
	return RoleAdmin, false
}

// Authorize decides whether the caller presenting token may call fullMethod.
// Public methods get ctx back unchanged whatever the token. Otherwise a
// missing or unknown token fails with codes.Unauthenticated, a role below
// RequiredRole fails with codes.PermissionDenied, and success returns ctx
// carrying the principal (see PrincipalFrom). Errors are gRPC status errors
// and the returned context is nil with them.
func (a *Authenticator) Authorize(ctx context.Context, fullMethod, token string) (context.Context, error) {
	required, public := RequiredRole(fullMethod)
	if public {
		return ctx, nil
	}
	if token == "" {
		return nil, status.Error(codes.Unauthenticated, "missing bearer token")
	}
	p, err := a.Authenticate(token)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid bearer token")
	}
	if !p.Can(required) {
		return nil, status.Errorf(codes.PermissionDenied, "%s %q may not call %s, which requires %s", p.Role, p.Name, fullMethod, required)
	}
	return WithPrincipal(ctx, p), nil
}
