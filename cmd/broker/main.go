package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
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

	log.Printf("messager listening on %s (maxRetries=%d, dispatchTimeout=%s)",
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
