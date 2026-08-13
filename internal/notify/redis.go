package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

// DefaultChannel is the Redis pub/sub channel used when none is configured.
const DefaultChannel = "controlplane:changes"

// Redis broadcasts events over Redis pub/sub. The go-redis client reconnects
// and resubscribes on its own; events published while a replica is
// disconnected are lost and recovered by the reconciler.
type Redis struct {
	client  redis.UniversalClient
	channel string
	log     *slog.Logger
}

// NewRedis returns a notifier on channel (DefaultChannel if empty).
func NewRedis(client redis.UniversalClient, channel string, log *slog.Logger) *Redis {
	if channel == "" {
		channel = DefaultChannel
	}
	if log == nil {
		log = slog.Default()
	}
	return &Redis{client: client, channel: channel, log: log}
}

func (r *Redis) Publish(ctx context.Context, ev Event) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if err := r.client.Publish(ctx, r.channel, payload).Err(); err != nil {
		return fmt.Errorf("redis publish: %w", err)
	}
	return nil
}

func (r *Redis) Run(ctx context.Context, h Handler) error {
	sub := r.client.Subscribe(ctx, r.channel)
	defer sub.Close()

	// Wait for the subscription to be confirmed so the caller knows events
	// published from now on will arrive.
	if _, err := sub.Receive(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("redis subscribe %s: %w", r.channel, err)
	}

	msgs := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return nil
			}
			var ev Event
			if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil || ev.Namespace == "" {
				r.log.Warn("ignoring malformed change event", "payload", msg.Payload, "error", err)
				continue
			}
			h(ev)
		}
	}
}
