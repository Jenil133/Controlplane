package server

import (
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/model"
)

// Values are stored as compact JSON produced by encoding/json (sorted object
// keys) and travel as google.protobuf.Value. So do the entry images in diffs
// and audit events.

func valueToJSON(field string, v *structpb.Value) (json.RawMessage, error) {
	if v == nil || v.GetKind() == nil {
		return nil, model.Invalidf("%s is required", field)
	}
	b, err := json.Marshal(v.AsInterface())
	if err != nil {
		return nil, model.Invalidf("%s: %v", field, err)
	}
	return b, nil
}

func optionalValueToJSON(field string, v *structpb.Value) (json.RawMessage, error) {
	if v == nil || v.GetKind() == nil {
		return nil, nil
	}
	return valueToJSON(field, v)
}

func jsonToValue(raw json.RawMessage) (*structpb.Value, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return structpb.NewValue(v)
}

func timestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// optionalTime converts a timestamp field that may be unset (zero time).
func optionalTime(field string, ts *timestamppb.Timestamp) (time.Time, error) {
	if ts == nil {
		return time.Time{}, nil
	}
	if err := ts.CheckValid(); err != nil {
		return time.Time{}, model.Invalidf("%s: %v", field, err)
	}
	return ts.AsTime(), nil
}

// durationFromProto converts a duration field that must be set: an unset
// duration would otherwise silently read as zero.
func durationFromProto(field string, d *durationpb.Duration) (time.Duration, error) {
	if d == nil {
		return 0, model.Invalidf("%s is required", field)
	}
	if err := d.CheckValid(); err != nil {
		return 0, model.Invalidf("%s: %v", field, err)
	}
	return d.AsDuration(), nil
}

func namespaceToProto(ns model.Namespace) *cpv1.Namespace {
	return &cpv1.Namespace{
		Name:        ns.Name,
		Description: ns.Description,
		Revision:    ns.Revision,
		CreatedAt:   timestamp(ns.CreatedAt),
		UpdatedAt:   timestamp(ns.UpdatedAt),
		CreatedBy:   ns.CreatedBy,
	}
}

func configToProto(c model.Config) (*cpv1.Config, error) {
	v, err := jsonToValue(c.Value)
	if err != nil {
		return nil, err
	}
	return &cpv1.Config{
		Key:         c.Key,
		Value:       v,
		Description: c.Description,
		Revision:    c.Revision,
		UpdatedAt:   timestamp(c.UpdatedAt),
		UpdatedBy:   c.UpdatedBy,
	}, nil
}

func flagToProto(f model.Flag) *cpv1.Flag {
	return &cpv1.Flag{
		Key:            f.Key,
		Enabled:        f.Enabled,
		Description:    f.Description,
		Revision:       f.Revision,
		UpdatedAt:      timestamp(f.UpdatedAt),
		UpdatedBy:      f.UpdatedBy,
		RolloutPercent: f.RolloutPercent,
		Salt:           f.Salt,
		Allowlist:      f.Allowlist,
		Rollout:        rolloutPlanToProto(f.Rollout),
	}
}

func rolloutPlanToProto(p *model.RolloutPlan) *cpv1.RolloutPlan {
	if p == nil {
		return nil
	}
	stages := make([]*cpv1.RolloutStage, len(p.Stages))
	for i, st := range p.Stages {
		// A zero duration (manual advance only) is sent as 0s rather than
		// left unset, so every stage reads the same way.
		stages[i] = &cpv1.RolloutStage{Percent: st.Percent, Duration: durationpb.New(st.Duration)}
	}
	return &cpv1.RolloutPlan{
		Stages:         stages,
		CurrentStage:   int32(p.CurrentStage),
		State:          rolloutStateToProto(p.State),
		StartedAt:      timestamp(p.StartedAt),
		StageStartedAt: timestamp(p.StageStartedAt),
		StartedBy:      p.StartedBy,
	}
}

func rolloutStateToProto(s model.RolloutState) cpv1.RolloutState {
	switch s {
	case model.RolloutActive:
		return cpv1.RolloutState_ROLLOUT_STATE_ACTIVE
	case model.RolloutPaused:
		return cpv1.RolloutState_ROLLOUT_STATE_PAUSED
	case model.RolloutCompleted:
		return cpv1.RolloutState_ROLLOUT_STATE_COMPLETED
	case model.RolloutAborted:
		return cpv1.RolloutState_ROLLOUT_STATE_ABORTED
	default:
		return cpv1.RolloutState_ROLLOUT_STATE_UNSPECIFIED
	}
}

// rolloutStagesFromProto converts the stages of a new rollout. An unset stage
// duration means zero: the stage only advances manually.
func rolloutStagesFromProto(in []*cpv1.RolloutStage) ([]model.RolloutStage, error) {
	out := make([]model.RolloutStage, len(in))
	for i, st := range in {
		out[i].Percent = st.GetPercent()
		if st.GetDuration() == nil {
			continue
		}
		d, err := durationFromProto(fmt.Sprintf("stage %d duration", i+1), st.GetDuration())
		if err != nil {
			return nil, err
		}
		out[i].Duration = d
	}
	return out, nil
}

func experimentToProto(e model.Experiment) (*cpv1.Experiment, error) {
	variants := make([]*cpv1.Variant, len(e.Variants))
	for i, v := range e.Variants {
		payload, err := jsonToValue(v.Payload)
		if err != nil {
			return nil, err
		}
		variants[i] = &cpv1.Variant{Name: v.Name, Weight: v.Weight, Payload: payload}
	}
	return &cpv1.Experiment{
		Key:         e.Key,
		Enabled:     e.Enabled,
		Description: e.Description,
		Salt:        e.Salt,
		Variants:    variants,
		Revision:    e.Revision,
		UpdatedAt:   timestamp(e.UpdatedAt),
		UpdatedBy:   e.UpdatedBy,
	}, nil
}

func variantsFromProto(in []*cpv1.Variant) ([]model.Variant, error) {
	out := make([]model.Variant, len(in))
	for i, v := range in {
		payload, err := optionalValueToJSON("variant "+v.GetName()+" payload", v.GetPayload())
		if err != nil {
			return nil, err
		}
		out[i] = model.Variant{Name: v.GetName(), Weight: v.GetWeight(), Payload: payload}
	}
	return out, nil
}

func rateLimitToProto(r model.RateLimit) *cpv1.RateLimit {
	return &cpv1.RateLimit{
		Key:               r.Key,
		Enabled:           r.Enabled,
		Description:       r.Description,
		RequestsPerSecond: r.RequestsPerSecond,
		Burst:             r.Burst,
		Revision:          r.Revision,
		UpdatedAt:         timestamp(r.UpdatedAt),
		UpdatedBy:         r.UpdatedBy,
	}
}

func circuitBreakerToProto(c model.CircuitBreaker) *cpv1.CircuitBreaker {
	return &cpv1.CircuitBreaker{
		Key:                  c.Key,
		Enabled:              c.Enabled,
		Description:          c.Description,
		FailureRateThreshold: c.FailureRateThreshold,
		MinRequests:          c.MinRequests,
		Window:               durationpb.New(c.Window),
		OpenDuration:         durationpb.New(c.OpenDuration),
		HalfOpenMaxRequests:  c.HalfOpenMaxRequests,
		Revision:             c.Revision,
		UpdatedAt:            timestamp(c.UpdatedAt),
		UpdatedBy:            c.UpdatedBy,
	}
}

// SnapshotToProto converts a store snapshot for the wire.
func SnapshotToProto(s model.Snapshot) (*cpv1.Snapshot, error) {
	out := &cpv1.Snapshot{
		Namespace:       s.Namespace,
		Revision:        s.Revision,
		UpdatedAt:       timestamp(s.UpdatedAt),
		Configs:         make([]*cpv1.Config, 0, len(s.Configs)),
		Flags:           make([]*cpv1.Flag, 0, len(s.Flags)),
		Experiments:     make([]*cpv1.Experiment, 0, len(s.Experiments)),
		RateLimits:      make([]*cpv1.RateLimit, 0, len(s.RateLimits)),
		CircuitBreakers: make([]*cpv1.CircuitBreaker, 0, len(s.CircuitBreakers)),
	}
	for _, c := range s.Configs {
		pc, err := configToProto(c)
		if err != nil {
			return nil, err
		}
		out.Configs = append(out.Configs, pc)
	}
	for _, f := range s.Flags {
		out.Flags = append(out.Flags, flagToProto(f))
	}
	for _, e := range s.Experiments {
		pe, err := experimentToProto(e)
		if err != nil {
			return nil, err
		}
		out.Experiments = append(out.Experiments, pe)
	}
	for _, r := range s.RateLimits {
		out.RateLimits = append(out.RateLimits, rateLimitToProto(r))
	}
	for _, c := range s.CircuitBreakers {
		out.CircuitBreakers = append(out.CircuitBreakers, circuitBreakerToProto(c))
	}
	return out, nil
}

// revisionToProto converts a history entry without its snapshot.
func revisionToProto(r model.Revision) *cpv1.Revision {
	return &cpv1.Revision{
		Namespace: r.Namespace,
		Revision:  r.Revision,
		Actor:     r.Actor,
		CreatedAt: timestamp(r.CreatedAt),
		Summary:   r.Summary,
	}
}

func changeTypeToProto(t model.ChangeType) cpv1.ChangeType {
	switch t {
	case model.ChangeAdded:
		return cpv1.ChangeType_CHANGE_TYPE_ADDED
	case model.ChangeModified:
		return cpv1.ChangeType_CHANGE_TYPE_MODIFIED
	case model.ChangeRemoved:
		return cpv1.ChangeType_CHANGE_TYPE_REMOVED
	default:
		return cpv1.ChangeType_CHANGE_TYPE_UNSPECIFIED
	}
}

func changesToProto(changes []model.Change) ([]*cpv1.Change, error) {
	out := make([]*cpv1.Change, len(changes))
	for i, c := range changes {
		before, err := jsonToValue(c.Before)
		if err != nil {
			return nil, err
		}
		after, err := jsonToValue(c.After)
		if err != nil {
			return nil, err
		}
		out[i] = &cpv1.Change{
			EntityType: c.EntityType,
			Key:        c.Key,
			Type:       changeTypeToProto(c.Type),
			Before:     before,
			After:      after,
		}
	}
	return out, nil
}

func auditEventToProto(e model.AuditEvent) (*cpv1.AuditEvent, error) {
	before, err := jsonToValue(e.Before)
	if err != nil {
		return nil, err
	}
	after, err := jsonToValue(e.After)
	if err != nil {
		return nil, err
	}
	return &cpv1.AuditEvent{
		Id:         e.ID,
		Namespace:  e.Namespace,
		Revision:   e.Revision,
		Actor:      e.Actor,
		Time:       timestamp(e.Time),
		Action:     e.Action,
		EntityType: e.EntityType,
		EntityKey:  e.EntityKey,
		Before:     before,
		After:      after,
		Message:    e.Message,
	}, nil
}
