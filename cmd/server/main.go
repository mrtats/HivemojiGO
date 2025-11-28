package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"hivemoji/internal/api"
	"hivemoji/internal/config"
	"hivemoji/internal/hive"
	"hivemoji/internal/processor"
	"hivemoji/internal/storage"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	store := storage.NewStore(pool)
	if err := store.EnsureSchema(ctx); err != nil {
		log.Fatalf("ensure schema: %v", err)
	}

	hiveClient := hive.NewClient(cfg.HiveRPCURL)
	proc := processor.New(store, hiveClient)

	go ingestLoop(ctx, proc, store, cfg)

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Logger(), middleware.Recover(), middleware.CORS())

	apiServer := api.New(store)
	apiServer.Register(e)

	webDir := assetDir()
	e.File("/", filepath.Join(webDir, "index.html"))
	e.Static("/", webDir)

	go func() {
		log.Printf("listening on %s", cfg.ServerAddr)
		if err := e.Start(cfg.ServerAddr); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
}

func assetDir() string {
	exePath, err := os.Executable()
	if err == nil {
		exeDir := filepath.Dir(exePath)
		candidate := filepath.Join(exeDir, "web")
		if _, err := os.Stat(filepath.Join(candidate, "index.html")); err == nil {
			return candidate
		}
	}

	cwd, _ := os.Getwd()
	candidate := filepath.Join(cwd, "web")
	if _, err := os.Stat(filepath.Join(candidate, "index.html")); err == nil {
		return candidate
	}

	log.Fatal("web assets not found (missing web/index.html)")
	return ""
}

func ingestLoop(ctx context.Context, proc *processor.Processor, store *storage.Store, cfg config.Config) {
	last, err := store.LastBlock(ctx)
	if err != nil {
		log.Printf("read last block: %v", err)
	}

	current := cfg.StartBlock
	if last > 0 && last+1 > current {
		current = last + 1
	}

	log.Printf("starting ingestion from block %d", current)

	for {
		select {
		case <-ctx.Done():
			log.Println("ingest loop stopping")
			return
		default:
		}

		block, err := proc.FetchBlock(ctx, current)
		if err != nil {
			log.Printf("fetch block %d: %v", current, err)
			time.Sleep(cfg.PollInterval)
			continue
		}
		if block == nil {
			time.Sleep(cfg.PollInterval)
			continue
		}

		if err := proc.ProcessBlock(ctx, block); err != nil {
			log.Printf("process block %d: %v", current, err)
			time.Sleep(cfg.PollInterval)
			continue
		}

		current++
	}
}
