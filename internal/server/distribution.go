package server

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	cpv1 "github.com/Jenil133/Controlplane/gen/controlplane/v1"
	"github.com/Jenil133/Controlplane/internal/hub"
	"github.com/Jenil133/Controlplane/internal/model"
)

type distributionService struct {
	cpv1.UnimplementedDistributionServiceServer
	*Server
}

func (d *distributionService) GetSnapshot(ctx context.Context, req *cpv1.GetSnapshotRequest) (*cpv1.GetSnapshotResponse, error) {
	if err := model.ValidateNamespaceName(req.GetNamespace()); err != nil {
		return nil, d.toStatus(err)
	}
	snap, err := d.loadSnapshot(ctx, req.GetNamespace())
	if err != nil {
		return nil, d.toStatus(err)
	}
	return &cpv1.GetSnapshotResponse{Snapshot: snap}, nil
}

func (d *distributionService) Watch(req *cpv1.WatchRequest, stream grpc.ServerStreamingServer[cpv1.WatchResponse]) error {
	ctx := stream.Context()
	if err := model.ValidateNamespaceName(req.GetNamespace()); err != nil {
		return d.toStatus(err)
	}
	sub, err := d.hub.Subscribe(ctx, req.GetNamespace(), req.GetKnownRevision())
	if err != nil {
		return d.toStatus(err)
	}
	defer sub.Close()

	log := d.log.With("namespace", req.GetNamespace(), "client_id", req.GetClientId())
	if p, ok := peer.FromContext(ctx); ok {
		log = log.With("peer", p.Addr.String())
	}
	log.Info("watch started", "known_revision", req.GetKnownRevision())
	d.observer.WatchStarted(req.GetNamespace())
	defer d.observer.WatchEnded(req.GetNamespace())
	opened := time.Now()

	for {
		snap, err := sub.Next(ctx)
		if err != nil {
			if errors.Is(err, hub.ErrClosed) {
				log.Info("watch ended by server shutdown")
			} else {
				log.Info("watch ended", "reason", err)
			}
			return d.toStatus(err)
		}
		if err := stream.Send(&cpv1.WatchResponse{Snapshot: snap}); err != nil {
			log.Info("watch ended", "reason", err)
			return err
		}
		// Only changes committed after the stream opened count as pushes. The
		// catch-up snapshot a new or reconnecting watcher gets first was
		// committed whenever the namespace last changed, so its age says
		// nothing about propagation and would swamp the lag histogram after
		// every restart.
		if committed := snap.GetUpdatedAt().AsTime(); committed.After(opened) {
			d.observer.SnapshotPushed(req.GetNamespace(), max(0, time.Since(committed)))
		}
		log.Debug("snapshot pushed", "revision", snap.GetRevision())
	}
}
