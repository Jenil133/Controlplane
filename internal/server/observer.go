package server

import "time"

// ChangeSource says how a replica learned about a new namespace revision.
type ChangeSource string

const (
	// SourceWrite: a write committed on this replica.
	SourceWrite ChangeSource = "write"
	// SourceNotifier: an event from the notifier (Redis pub/sub).
	SourceNotifier ChangeSource = "notifier"
	// SourceReconcile: the periodic reconciler found watchers behind.
	SourceReconcile ChangeSource = "reconcile"
	// SourceRollout: the rollout controller advanced a stage.
	SourceRollout ChangeSource = "rollout"
)

// Observer receives server events, e.g. to export metrics. Implementations
// must be safe for concurrent use and must not block.
type Observer interface {
	WatchStarted(namespace string)
	WatchEnded(namespace string)
	// SnapshotPushed is called after a snapshot is sent on a Watch stream.
	// lag is the time from the change being committed to the send.
	SnapshotPushed(namespace string, lag time.Duration)
	// ChangeReceived is called when this replica learns about a revision.
	ChangeReceived(namespace string, source ChangeSource)
	// PublishFailed is called when a change event could not be broadcast.
	PublishFailed(namespace string)
}

// NopObserver ignores every event.
type NopObserver struct{}

func (NopObserver) WatchStarted(string)                  {}
func (NopObserver) WatchEnded(string)                    {}
func (NopObserver) SnapshotPushed(string, time.Duration) {}
func (NopObserver) ChangeReceived(string, ChangeSource)  {}
func (NopObserver) PublishFailed(string)                 {}
