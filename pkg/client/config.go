package client

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
)

// Config returns the value of the config key. The value is shared and must
// not be modified.
func (c *Client) Config(key string) (*structpb.Value, bool) {
	v, ok := c.current().configs[key]
	return v, ok
}

// String returns the config key if it is a JSON string, else def.
func (c *Client) String(key, def string) string {
	if s, ok := c.str(key); ok {
		return s
	}
	return def
}

// Int returns the config key if it is a JSON number with an integral value
// that fits in an int64, else def.
func (c *Client) Int(key string, def int64) int64 {
	// -2^63 and 2^63 are exact float64 values, so the range check is too.
	if f, ok := c.number(key); ok && f == math.Trunc(f) && f >= -(1<<63) && f < 1<<63 {
		return int64(f)
	}
	return def
}

// Float returns the config key if it is a JSON number, else def.
func (c *Client) Float(key string, def float64) float64 {
	if f, ok := c.number(key); ok {
		return f
	}
	return def
}

// Bool returns the config key if it is a JSON boolean, else def.
func (c *Client) Bool(key string, def bool) bool {
	if v, ok := c.Config(key); ok {
		if b, ok := v.GetKind().(*structpb.Value_BoolValue); ok {
			return b.BoolValue
		}
	}
	return def
}

// Duration returns the config key if it is a JSON string that
// time.ParseDuration accepts, such as "250ms" or "1m30s", else def.
func (c *Client) Duration(key string, def time.Duration) time.Duration {
	if s, ok := c.str(key); ok {
		if d, err := time.ParseDuration(s); err == nil {
			return d
		}
	}
	return def
}

// Decode stores the config key in out, as json.Unmarshal would store the
// value's JSON. A missing key is an error wrapping ErrNotFound.
func (c *Client) Decode(key string, out any) error {
	v, ok := c.Config(key)
	if !ok {
		return fmt.Errorf("client: config %q: %w", key, ErrNotFound)
	}
	b, err := json.Marshal(v.AsInterface())
	if err != nil {
		return fmt.Errorf("client: config %q: %w", key, err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("client: decode config %q: %w", key, err)
	}
	return nil
}

func (c *Client) str(key string) (string, bool) {
	if v, ok := c.Config(key); ok {
		if s, ok := v.GetKind().(*structpb.Value_StringValue); ok {
			return s.StringValue, true
		}
	}
	return "", false
}

func (c *Client) number(key string) (float64, bool) {
	if v, ok := c.Config(key); ok {
		if n, ok := v.GetKind().(*structpb.Value_NumberValue); ok {
			return n.NumberValue, true
		}
	}
	return 0, false
}
