package main

import (
	"log"
	"net"
	"os"

	"github.com/harsh3dev/messager/internal/api"
	"github.com/harsh3dev/messager/internal/queue"
	proto "github.com/harsh3dev/messager/proto/gen"
	"google.golang.org/grpc"
)

func main() {
	walDir := envOr("WAL_DIR", "data/wal")
	listenAddr := envOr("LISTEN_ADDR", ":50051")

	manager, err := queue.NewManager(walDir)
	if err != nil {
		log.Fatalf("queue manager: %v", err)
	}
	defer manager.Close()

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterBrokerServer(grpcServer, api.NewServer(manager))

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
