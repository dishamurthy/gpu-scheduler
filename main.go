package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"gpu-scheduler/internal/cache"
	"gpu-scheduler/internal/db"
	"gpu-scheduler/internal/handlers"
	"gpu-scheduler/internal/metrics"
	"gpu-scheduler/internal/scheduler"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := db.NewPool(ctx, env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/gpuscheduler?sslmode=disable"))
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	rdb, err := cache.NewClient(env("REDIS_URL", "redis://localhost:6379"))
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()

	metrics.Init()

	sched := scheduler.New(pool, rdb)
	go sched.Run(ctx)

	r := handlers.NewRouter(pool, rdb, sched)

	srv := &http.Server{Addr: ":8080", Handler: r}
	go func() {
		log.Println("API listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")
	srv.Shutdown(context.Background())
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
