package server

import (
	"encoding/json"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/model"
)

// Values are stored as compact JSON produced by encoding/json (sorted object
// keys) and travel as google.protobuf.Value.

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

func namespaceToProto(ns model.Namespace) *cpv1.Namespace {
	return &cpv1.Namespace{
		Name:        ns.Name,
		Description: ns.Description,
		Revision:    ns.Revision,
		CreatedAt:   timestamp(ns.CreatedAt),
		UpdatedAt:   timestamp(ns.UpdatedAt),
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
		Key:         f.Key,
		Enabled:     f.Enabled,
		Description: f.Description,
		Revision:    f.Revision,
		UpdatedAt:   timestamp(f.UpdatedAt),
		UpdatedBy:   f.UpdatedBy,
	}
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

// SnapshotToProto converts a store snapshot for the wire.
func SnapshotToProto(s model.Snapshot) (*cpv1.Snapshot, error) {
	out := &cpv1.Snapshot{
		Namespace:   s.Namespace,
		Revision:    s.Revision,
		UpdatedAt:   timestamp(s.UpdatedAt),
		Configs:     make([]*cpv1.Config, 0, len(s.Configs)),
		Flags:       make([]*cpv1.Flag, 0, len(s.Flags)),
		Experiments: make([]*cpv1.Experiment, 0, len(s.Experiments)),
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
	return out, nil
}
