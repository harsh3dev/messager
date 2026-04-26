package main

import (
	"context"
	"log"
	"net"
	"os"

	"github.com/harsh3dev/messager/internal/ackmgr"
	"github.com/harsh3dev/messager/internal/api"
	"github.com/harsh3dev/messager/internal/connmgr"
	"github.com/harsh3dev/messager/internal/dispatcher"
	"github.com/harsh3dev/messager/internal/queue"
	proto "github.com/harsh3dev/messager/proto/gen"
	"google.golang.org/grpc"
)

func main() {
	walDir := envOr("WAL_DIR", "data/wal")
	listenAddr := envOr("LISTEN_ADDR", ":50051")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager, err := queue.NewManager(walDir)
	if err != nil {
		log.Fatalf("queue manager: %v", err)
	}
	defer manager.Close()

	registry := connmgr.NewRegistry()
	ackMgr := ackmgr.NewAckManager(manager, registry)
	disp := dispatcher.NewDispatcher(manager, registry, ackMgr)

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterBrokerServer(grpcServer, api.NewServer(ctx, manager, registry, ackMgr, disp))

	log.Printf("messager listening on %s", listenAddr)
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
