package server

import (
	"context"
	"fmt"
	"time"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/model"
	"github.com/Jenil133/Controlplane/internal/rollout"
	"github.com/Jenil133/Controlplane/internal/store"
)

// transition is one of the rollout package's pure state transitions.
type transition func(f model.Flag, actor string, now time.Time) (model.Flag, error)

// ignoringActor adapts the transitions that record no actor.
func ignoringActor(fn func(model.Flag, time.Time) (model.Flag, error)) transition {
	return func(f model.Flag, _ string, now time.Time) (model.Flag, error) { return fn(f, now) }
}

func (a *adminService) StartRollout(ctx context.Context, req *cpv1.StartRolloutRequest) (*cpv1.StartRolloutResponse, error) {
	stages, err := rolloutStagesFromProto(req.GetStages())
	if err != nil {
		return nil, a.toStatus(err)
	}
	// rollout.Start checks the stages too, but only after the flag is read:
	// a malformed request should fail as such whatever the flag's state.
	if err := model.ValidateRolloutStages(stages); err != nil {
		return nil, a.toStatus(err)
	}
	start := func(f model.Flag, actor string, now time.Time) (model.Flag, error) {
		return rollout.Start(f, stages, actor, now)
	}
	f, err := a.updateRollout(ctx, req.GetNamespace(), req.GetFlag(), "start", model.ActionRolloutStart, start)
	if err != nil {
		return nil, err
	}
	return &cpv1.StartRolloutResponse{Flag: f}, nil
}

func (a *adminService) AdvanceRollout(ctx context.Context, req *cpv1.AdvanceRolloutRequest) (*cpv1.AdvanceRolloutResponse, error) {
	f, err := a.updateRollout(ctx, req.GetNamespace(), req.GetFlag(), "advance", model.ActionRolloutAdvance, ignoringActor(rollout.Advance))
	if err != nil {
		return nil, err
	}
	return &cpv1.AdvanceRolloutResponse{Flag: f}, nil
}

func (a *adminService) PauseRollout(ctx context.Context, req *cpv1.PauseRolloutRequest) (*cpv1.PauseRolloutResponse, error) {
	f, err := a.updateRollout(ctx, req.GetNamespace(), req.GetFlag(), "pause", model.ActionRolloutPause, ignoringActor(rollout.Pause))
	if err != nil {
		return nil, err
	}
	return &cpv1.PauseRolloutResponse{Flag: f}, nil
}

func (a *adminService) ResumeRollout(ctx context.Context, req *cpv1.ResumeRolloutRequest) (*cpv1.ResumeRolloutResponse, error) {
	f, err := a.updateRollout(ctx, req.GetNamespace(), req.GetFlag(), "resume", model.ActionRolloutResume, ignoringActor(rollout.Resume))
	if err != nil {
		return nil, err
	}
	return &cpv1.ResumeRolloutResponse{Flag: f}, nil
}

func (a *adminService) AbortRollout(ctx context.Context, req *cpv1.AbortRolloutRequest) (*cpv1.AbortRolloutResponse, error) {
	f, err := a.updateRollout(ctx, req.GetNamespace(), req.GetFlag(), "abort", model.ActionRolloutAbort, ignoringActor(rollout.Abort))
	if err != nil {
		return nil, err
	}
	return &cpv1.AbortRolloutResponse{Flag: f}, nil
}

// updateRollout applies a rollout transition to a flag and returns the
// result or a status error. The write is a compare-and-swap on the revision
// read, retried from a fresh read when it loses a race, e.g. with a rollout
// controller advancing the same flag: the transition must judge the state it
// actually replaces.
func (a *adminService) updateRollout(ctx context.Context, namespace, key, verb, action string, apply transition) (*cpv1.Flag, error) {
	who, err := a.prepareWrite(ctx, namespace)
	if err != nil {
		return nil, a.toStatus(err)
	}
	if err := model.ValidateKey(key); err != nil {
		return nil, a.toStatus(err)
	}
	out, err := retryOnConflict(func() (model.Flag, error) {
		cur, err := a.store.GetFlag(ctx, namespace, key)
		if err != nil {
			return model.Flag{}, err
		}
		next, err := apply(cur, who, a.now())
		if err != nil {
			return model.Flag{}, err
		}
		return a.store.PutFlag(ctx, namespace, next, store.WriteOptions{
			Actor:            who,
			ExpectedRevision: cur.Revision,
			Action:           action,
			Message:          fmt.Sprintf("%s rollout of flag %s: %s", verb, key, rollout.Describe(next.Rollout)),
		})
	})
	if err != nil {
		return nil, a.toStatus(err)
	}
	a.log.Info("rollout updated", "namespace", namespace, "flag", key, "action", action, "stage", rollout.Describe(out.Rollout),
		"state", out.Rollout.State, "rollout_percent", out.RolloutPercent, "revision", out.Revision, "actor", who)
	a.changed(ctx, namespace, out.Revision, SourceWrite)
	return flagToProto(out), nil
}
