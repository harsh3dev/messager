.PHONY: proto tidy build test

proto:
	protoc -I proto \
	  --go_out=proto/gen --go_opt=paths=source_relative \
	  --go-grpc_out=proto/gen --go-grpc_opt=paths=source_relative \
	  proto/broker.proto

tidy:
	go mod tidy

build:
	go build ./...

test:
	go test ./... -race