package client

import "google.golang.org/protobuf/types/known/structpb"

// Assignment is the variant of an experiment a unit is enrolled in.
type Assignment struct {
	Experiment string
	Variant    string
	// Payload is the variant's payload, nil if it has none. It is shared and
	// must not be modified.
	Payload *structpb.Value
}

// IsEnabled reports whether the flag flagKey is on for unitID (a user ID,
// account ID, ...). A flag that does not exist is off. The answer depends
// only on the flag and unitID, so every client serving the same revision
// gives the same one; see package eval for the rules.
func (c *Client) IsEnabled(flagKey, unitID string) bool {
	return c.current().flags[flagKey].Enabled(unitID)
}

// Variant returns the variant of the experiment experimentKey that unitID is
// assigned to. It reports false when unitID is not enrolled: the experiment
// does not exist or is disabled, unitID is empty, or no variant has weight.
// Like IsEnabled, the assignment is the same in every client.
func (c *Client) Variant(experimentKey, unitID string) (Assignment, bool) {
	v, ok := c.current().experiments[experimentKey].Assign(unitID)
	if !ok {
		return Assignment{}, false
	}
	return Assignment{Experiment: experimentKey, Variant: v.GetName(), Payload: v.GetPayload()}, true
}
