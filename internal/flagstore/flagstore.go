// Package flagstore holds the domain logic for feature flags: storage,
// mutation, sticky-rollout evaluation, and change notification. It knows
// nothing about gRPC or protobuf so it can be unit tested in isolation.
package flagstore

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/mehdi/feature-flag-service/flagstore")

var (
	ErrNotFound      = errors.New("flag not found")
	ErrAlreadyExists = errors.New("flag already exists")
)

// Flag is the domain representation of a feature flag.
type Flag struct {
	Key               string
	Description       string
	Enabled           bool
	RolloutPercentage int32
	TargetingRules    []string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type EventType int

const (
	EventCreated EventType = iota
	EventUpdated
	EventDeleted
)

type Event struct {
	Type EventType
	Flag Flag
}

// Store is an in-memory, concurrency-safe flag repository with pub/sub for
// change notifications. A real deployment would back this with a database
// and an outbox or CDC stream, but the interface would look the same.
type Store struct {
	mu    sync.RWMutex
	flags map[string]Flag

	subMu     sync.Mutex
	subs      map[int]chan Event
	nextSubID int
}

func NewStore() *Store {
	return &Store{
		flags: make(map[string]Flag),
		subs:  make(map[int]chan Event),
	}
}

func (s *Store) Create(ctx context.Context, f Flag) (Flag, error) {
	ctx, span := tracer.Start(ctx, "flagstore.Create", trace.WithAttributes(
		attribute.String("flag.key", f.Key),
	))
	defer span.End()
	_ = ctx

	now := time.Now().UTC()
	f.CreatedAt = now
	f.UpdatedAt = now

	s.mu.Lock()
	if _, exists := s.flags[f.Key]; exists {
		s.mu.Unlock()
		span.SetStatus(otelcodes.Error, "flag already exists")
		return Flag{}, ErrAlreadyExists
	}
	s.flags[f.Key] = f
	s.mu.Unlock()

	s.publish(Event{Type: EventCreated, Flag: f})
	return f, nil
}

func (s *Store) Get(ctx context.Context, key string) (Flag, error) {
	_, span := tracer.Start(ctx, "flagstore.Get", trace.WithAttributes(
		attribute.String("flag.key", key),
	))
	defer span.End()

	s.mu.RLock()
	f, ok := s.flags[key]
	s.mu.RUnlock()
	if !ok {
		span.SetStatus(otelcodes.Error, "flag not found")
		return Flag{}, ErrNotFound
	}
	return f, nil
}

func (s *Store) List(ctx context.Context) ([]Flag, error) {
	_, span := tracer.Start(ctx, "flagstore.List")
	defer span.End()

	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Flag, 0, len(s.flags))
	for _, f := range s.flags {
		out = append(out, f)
	}
	span.SetAttributes(attribute.Int("flag.count", len(out)))
	return out, nil
}

// Update applies a partial mutation to an existing flag. Pointer fields left
// nil are left unchanged; TargetingRules, when non-nil, fully replaces the
// existing rule set.
type Update struct {
	Enabled           *bool
	Description       *string
	RolloutPercentage *int32
	TargetingRules    []string
}

func (s *Store) Update(ctx context.Context, key string, u Update) (Flag, error) {
	ctx, span := tracer.Start(ctx, "flagstore.Update", trace.WithAttributes(
		attribute.String("flag.key", key),
	))
	defer span.End()
	_ = ctx

	s.mu.Lock()
	f, ok := s.flags[key]
	if !ok {
		s.mu.Unlock()
		span.SetStatus(otelcodes.Error, "flag not found")
		return Flag{}, ErrNotFound
	}

	if u.Enabled != nil {
		f.Enabled = *u.Enabled
	}
	if u.Description != nil {
		f.Description = *u.Description
	}
	if u.RolloutPercentage != nil {
		f.RolloutPercentage = *u.RolloutPercentage
	}
	if u.TargetingRules != nil {
		f.TargetingRules = u.TargetingRules
	}
	f.UpdatedAt = time.Now().UTC()
	s.flags[key] = f
	s.mu.Unlock()

	s.publish(Event{Type: EventUpdated, Flag: f})
	return f, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	_, span := tracer.Start(ctx, "flagstore.Delete", trace.WithAttributes(
		attribute.String("flag.key", key),
	))
	defer span.End()

	s.mu.Lock()
	f, ok := s.flags[key]
	if !ok {
		s.mu.Unlock()
		span.SetStatus(otelcodes.Error, "flag not found")
		return ErrNotFound
	}
	delete(s.flags, key)
	s.mu.Unlock()

	s.publish(Event{Type: EventDeleted, Flag: f})
	return nil
}

// Evaluate decides whether key is enabled for subjectID. Targeting rules
// win outright; otherwise the subject is bucketed deterministically into
// [0, 100) so the same subject always gets the same rollout decision for a
// given flag, even across process restarts.
func (s *Store) Evaluate(ctx context.Context, key, subjectID string) (enabled bool, reason string, err error) {
	_, span := tracer.Start(ctx, "flagstore.Evaluate", trace.WithAttributes(
		attribute.String("flag.key", key),
		attribute.String("subject.id", subjectID),
	))
	defer span.End()

	s.mu.RLock()
	f, ok := s.flags[key]
	s.mu.RUnlock()
	if !ok {
		span.SetStatus(otelcodes.Error, "flag not found")
		return false, "flag_not_found", ErrNotFound
	}

	if !f.Enabled {
		span.SetAttributes(attribute.String("evaluation.reason", "flag_disabled"))
		return false, "flag_disabled", nil
	}

	for _, rule := range f.TargetingRules {
		if rule == subjectID {
			span.SetAttributes(attribute.String("evaluation.reason", "targeting_rule"))
			return true, "targeting_rule", nil
		}
	}

	bucket := bucketFor(key, subjectID)
	span.SetAttributes(
		attribute.Int("evaluation.bucket", bucket),
		attribute.String("evaluation.reason", "rollout_percentage"),
	)
	return bucket < int(f.RolloutPercentage), "rollout_percentage", nil
}

// bucketFor deterministically maps (key, subjectID) to [0, 100).
func bucketFor(key, subjectID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key + "\x00" + subjectID))
	return int(h.Sum32() % 100)
}

// Subscribe registers a new change listener. The returned cancel func must
// be called (typically via defer) to release the subscription and avoid
// leaking the channel.
func (s *Store) Subscribe() (<-chan Event, func()) {
	s.subMu.Lock()
	id := s.nextSubID
	s.nextSubID++
	ch := make(chan Event, 16)
	s.subs[id] = ch
	s.subMu.Unlock()

	cancel := func() {
		s.subMu.Lock()
		defer s.subMu.Unlock()
		if _, ok := s.subs[id]; ok {
			delete(s.subs, id)
			close(ch)
		}
	}
	return ch, cancel
}

// publish fans an event out to all subscribers. Slow subscribers have
// events dropped rather than blocking writers or each other -- WatchFlags
// is a best-effort feed, not a durable log.
func (s *Store) publish(ev Event) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for _, ch := range s.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}
