package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	"github.com/maddiesch/go-sterling"
	"golang.org/x/sync/errgroup"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	_, f, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(f), "jobs.db")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	client, err := sterling.New(ctx, sterling.WithDatabaseFile(path))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	client.Register("log-event", sterling.ValueWorker(func(ctx context.Context, job *sterling.Job, message string) error {
		if err := sterling.ExtendLease(ctx, job); err != nil {
			return err
		}

		slog.InfoContext(ctx, "Log Event", slog.String("message", message))

		return nil
	}))

	client.RegisterFunc("flaky-job", func(ctx context.Context, job *sterling.Job) error {
		if job.Attempt == 1 {
			return fmt.Errorf("simulated error on first attempt")
		}
		return nil
	})

	// _ = client.Push(ctx, "low", "flaky-job")

	eGroup, ctx := errgroup.WithContext(ctx)

	eGroup.Go(func() error {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		started := time.Now().Format(time.RFC3339)
		var count int

		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				message := fmt.Sprintf("[%s] Event %d", started, count)
				count++

				options := []sterling.PushOption{
					sterling.PushJSON(message),
				}

				if count%5 == 0 {
					options = append(options, func(p *sterling.Push) error {
						p.VisibleAt = time.Now().Add(2 * time.Minute)
						return nil
					})
				}

				if err := client.Push(ctx, "default", "log-event", options...); err != nil {
					slog.ErrorContext(ctx, "failed to push job", slog.String("error", err.Error()))
					return err
				}
			}
		}
	})

	eGroup.Go(func() error {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if err := client.Cleanup(ctx); err != nil {
					slog.ErrorContext(ctx, "failed to run Client Cleanup", slog.String("error", err.Error()))
					return err
				}
			}
		}
	})

	eGroup.Go(func() error {
		return client.Run(ctx, []string{"default", "low"}, 2)
	})

	if err := eGroup.Wait(); err != nil {
		slog.ErrorContext(ctx, "error occurred", slog.String("error", err.Error()))
	}

	queues, err := client.ListQueue(context.Background())
	if err != nil {
		panic(err)
	}

	var results []*sterling.QueueStat

	for _, queue := range queues {
		stats, err := client.LoadQueueStat(context.Background(), queue)
		if err != nil {
			panic(err)
		}

		results = append(results, stats)
	}

	enc := json.NewEncoder(os.Stderr)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		panic(err)
	}
}
