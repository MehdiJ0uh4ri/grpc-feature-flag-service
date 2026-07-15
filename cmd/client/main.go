// Command client is a small demo CLI that exercises the FeatureFlagService
// and shows context/trace propagation end-to-end: it starts a span here,
// calls the server over gRPC, and that same trace_id shows up in the
// server's logs and in the exported trace.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	featureflagv1 "github.com/mehdi/feature-flag-service/gen/featureflag/v1"
	"github.com/mehdi/feature-flag-service/internal/config"
	"github.com/mehdi/feature-flag-service/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg := config.Load()
	cfg.ServiceName = "feature-flag-client"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	shutdown, err := telemetry.InitTracerProvider(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer shutdown(context.Background())

	target := "localhost:50051"
	if v := os.Getenv("SERVER_ADDR"); v != "" {
		target = v
	}

	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return fmt.Errorf("dial %s: %w", target, err)
	}
	defer conn.Close()

	client := featureflagv1.NewFeatureFlagServiceClient(conn)

	tracer := otel.Tracer("github.com/mehdi/feature-flag-service/client")
	ctx, span := tracer.Start(ctx, "demo-run")
	defer span.End()

	flag, err := client.CreateFlag(ctx, &featureflagv1.CreateFlagRequest{
		Key:               "new-checkout-flow",
		Description:       "Rolls out the redesigned checkout flow",
		Enabled:           true,
		RolloutPercentage: 25,
		TargetingRules:    []string{"beta-tester-1"},
	})
	if err != nil {
		return fmt.Errorf("create flag: %w", err)
	}
	fmt.Printf("created flag: %s (enabled=%v, rollout=%d%%)\n", flag.GetKey(), flag.GetEnabled(), flag.GetRolloutPercentage())

	for _, subject := range []string{"beta-tester-1", "user-42", "user-43"} {
		eval, err := client.EvaluateFlag(ctx, &featureflagv1.EvaluateFlagRequest{
			Key:       flag.GetKey(),
			SubjectId: subject,
		})
		if err != nil {
			return fmt.Errorf("evaluate flag for %s: %w", subject, err)
		}
		fmt.Printf("evaluate(%s) => enabled=%v reason=%s\n", subject, eval.GetEnabled(), eval.GetReason())
	}

	watchCtx, watchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer watchCancel()
	stream, err := client.WatchFlags(watchCtx, &featureflagv1.WatchFlagsRequest{})
	if err != nil {
		return fmt.Errorf("watch flags: %w", err)
	}

	updated := false
	go func() {
		time.Sleep(200 * time.Millisecond)
		_, uerr := client.UpdateFlag(ctx, &featureflagv1.UpdateFlagRequest{
			Key: flag.GetKey(),
		})
		if uerr != nil {
			fmt.Println("background update failed:", uerr)
		}
	}()

	for {
		ev, err := stream.Recv()
		if err == io.EOF || watchCtx.Err() != nil {
			break
		}
		if err != nil {
			break
		}
		fmt.Printf("watch event: type=%s flag=%s\n", ev.GetType(), ev.GetFlag().GetKey())
		updated = true
	}
	if !updated {
		fmt.Println("watch stream closed with no events observed (server may not be reachable yet)")
	}

	return nil
}
