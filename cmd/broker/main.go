package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/harsh3dev/messager/internal/ackmgr"
	"github.com/harsh3dev/messager/internal/api"
	"github.com/harsh3dev/messager/internal/connmgr"
	"github.com/harsh3dev/messager/internal/dispatcher"
	"github.com/harsh3dev/messager/internal/queue"
	"github.com/harsh3dev/messager/internal/retry"
	proto "github.com/harsh3dev/messager/proto/gen"
	"google.golang.org/grpc"
)

func main() {
	walDir := envOr("WAL_DIR", "data/wal")
	listenAddr := envOr("LISTEN_ADDR", ":50051")
	maxRetries := int32(envOrInt("MAX_RETRIES", 3))
	dispatchTimeout := envOrDuration("DISPATCH_TIMEOUT", 30*time.Second)
	scanInterval := envOrDuration("SCAN_INTERVAL", 5*time.Second)
	shutdownTimeout := envOrDuration("SHUTDOWN_TIMEOUT", 15*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager, err := queue.NewManager(walDir)
	if err != nil {
		log.Fatalf("queue manager: %v", err)
	}
	defer manager.Close()

	registry := connmgr.NewRegistry()
	ackMgr := ackmgr.NewAckManager(manager, registry, maxRetries)
	disp := dispatcher.NewDispatcher(manager, registry, ackMgr)
	scanner := retry.NewScanner(ackMgr, scanInterval, dispatchTimeout)

	go scanner.Run(ctx)

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterBrokerServer(grpcServer, api.NewServer(ctx, manager, registry, ackMgr, disp))

	// Graceful shutdown on SIGINT or SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received %s, shutting down…", sig)

		// Stop accepting new Publish requests immediately.
		manager.BeginShutdown()

		// Stop accepting new gRPC connections and wait for ongoing RPCs to finish.
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(shutdownTimeout):
			log.Println("graceful stop timed out, forcing")
			grpcServer.Stop()
		}

		// Wait for in-flight messages to be ACKed/NACKed before closing the WAL.
		if !ackMgr.WaitDrained(shutdownTimeout) {
			log.Println("warning: in-flight messages not fully drained; they will be redelivered on restart")
		}

		cancel()
	}()

	log.Printf("messager listening on %s (maxRetries=%d dispatchTimeout=%s)",
		listenAddr, maxRetries, dispatchTimeout)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if n, err := strconv.Atoi(value); err == nil {
			return n
		}
	}
	return fallback
}

func envOrDuration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return fallback
}
