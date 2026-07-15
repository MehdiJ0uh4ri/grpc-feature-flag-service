// Package server implements the FeatureFlagService gRPC API on top of the
// in-memory flagstore.Store, translating between proto and domain types and
// mapping domain errors to gRPC status codes.
package server

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	featureflagv1 "github.com/mehdi/feature-flag-service/gen/featureflag/v1"
	"github.com/mehdi/feature-flag-service/internal/flagstore"
)

type Server struct {
	featureflagv1.UnimplementedFeatureFlagServiceServer

	store  *flagstore.Store
	logger *slog.Logger
}

func New(store *flagstore.Store, logger *slog.Logger) *Server {
	return &Server{store: store, logger: logger}
}

func (s *Server) CreateFlag(ctx context.Context, req *featureflagv1.CreateFlagRequest) (*featureflagv1.Flag, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	if req.GetRolloutPercentage() < 0 || req.GetRolloutPercentage() > 100 {
		return nil, status.Error(codes.InvalidArgument, "rollout_percentage must be between 0 and 100")
	}

	f, err := s.store.Create(ctx, flagstore.Flag{
		Key:               req.GetKey(),
		Description:       req.GetDescription(),
		Enabled:           req.GetEnabled(),
		RolloutPercentage: req.GetRolloutPercentage(),
		TargetingRules:    req.GetTargetingRules(),
	})
	if err != nil {
		if errors.Is(err, flagstore.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "flag %q already exists", req.GetKey())
		}
		return nil, status.Errorf(codes.Internal, "create flag: %v", err)
	}
	return toProto(f), nil
}

func (s *Server) GetFlag(ctx context.Context, req *featureflagv1.GetFlagRequest) (*featureflagv1.Flag, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	f, err := s.store.Get(ctx, req.GetKey())
	if err != nil {
		if errors.Is(err, flagstore.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "flag %q not found", req.GetKey())
		}
		return nil, status.Errorf(codes.Internal, "get flag: %v", err)
	}
	return toProto(f), nil
}

func (s *Server) ListFlags(ctx context.Context, _ *featureflagv1.ListFlagsRequest) (*featureflagv1.ListFlagsResponse, error) {
	flags, err := s.store.List(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list flags: %v", err)
	}
	resp := &featureflagv1.ListFlagsResponse{Flags: make([]*featureflagv1.Flag, 0, len(flags))}
	for _, f := range flags {
		resp.Flags = append(resp.Flags, toProto(f))
	}
	return resp, nil
}

func (s *Server) UpdateFlag(ctx context.Context, req *featureflagv1.UpdateFlagRequest) (*featureflagv1.Flag, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	if req.RolloutPercentage != nil && (*req.RolloutPercentage < 0 || *req.RolloutPercentage > 100) {
		return nil, status.Error(codes.InvalidArgument, "rollout_percentage must be between 0 and 100")
	}

	f, err := s.store.Update(ctx, req.GetKey(), flagstore.Update{
		Enabled:           req.Enabled,
		Description:       req.Description,
		RolloutPercentage: req.RolloutPercentage,
		TargetingRules:    req.TargetingRules,
	})
	if err != nil {
		if errors.Is(err, flagstore.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "flag %q not found", req.GetKey())
		}
		return nil, status.Errorf(codes.Internal, "update flag: %v", err)
	}
	return toProto(f), nil
}

func (s *Server) DeleteFlag(ctx context.Context, req *featureflagv1.DeleteFlagRequest) (*featureflagv1.DeleteFlagResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	if err := s.store.Delete(ctx, req.GetKey()); err != nil {
		if errors.Is(err, flagstore.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "flag %q not found", req.GetKey())
		}
		return nil, status.Errorf(codes.Internal, "delete flag: %v", err)
	}
	return &featureflagv1.DeleteFlagResponse{Deleted: true}, nil
}

func (s *Server) EvaluateFlag(ctx context.Context, req *featureflagv1.EvaluateFlagRequest) (*featureflagv1.EvaluateFlagResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	if req.GetSubjectId() == "" {
		return nil, status.Error(codes.InvalidArgument, "subject_id is required")
	}

	enabled, reason, err := s.store.Evaluate(ctx, req.GetKey(), req.GetSubjectId())
	if err != nil && !errors.Is(err, flagstore.ErrNotFound) {
		return nil, status.Errorf(codes.Internal, "evaluate flag: %v", err)
	}
	// A missing flag is a valid (disabled) evaluation result, not an RPC
	// error: callers evaluating flags shouldn't have to special-case
	// "doesn't exist yet" vs. "exists but off".
	return &featureflagv1.EvaluateFlagResponse{
		Key:     req.GetKey(),
		Enabled: enabled,
		Reason:  reason,
	}, nil
}

func (s *Server) WatchFlags(_ *featureflagv1.WatchFlagsRequest, stream featureflagv1.FeatureFlagService_WatchFlagsServer) error {
	ctx := stream.Context()
	events, cancel := s.store.Subscribe()
	defer cancel()

	s.logger.InfoContext(ctx, "watch stream opened")
	defer s.logger.InfoContext(ctx, "watch stream closed")

	for {
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if err := stream.Send(&featureflagv1.FlagEvent{
				Type: toProtoEventType(ev.Type),
				Flag: toProto(ev.Flag),
			}); err != nil {
				return err
			}
		}
	}
}

func toProto(f flagstore.Flag) *featureflagv1.Flag {
	return &featureflagv1.Flag{
		Key:               f.Key,
		Description:       f.Description,
		Enabled:           f.Enabled,
		RolloutPercentage: f.RolloutPercentage,
		TargetingRules:    f.TargetingRules,
		CreatedAt:         timestamppb.New(f.CreatedAt),
		UpdatedAt:         timestamppb.New(f.UpdatedAt),
	}
}

func toProtoEventType(t flagstore.EventType) featureflagv1.FlagEventType {
	switch t {
	case flagstore.EventCreated:
		return featureflagv1.FlagEventType_FLAG_EVENT_TYPE_CREATED
	case flagstore.EventUpdated:
		return featureflagv1.FlagEventType_FLAG_EVENT_TYPE_UPDATED
	case flagstore.EventDeleted:
		return featureflagv1.FlagEventType_FLAG_EVENT_TYPE_DELETED
	default:
		return featureflagv1.FlagEventType_FLAG_EVENT_TYPE_UNSPECIFIED
	}
}
