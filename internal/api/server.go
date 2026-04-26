package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"time"

	"github.com/harsh3dev/messager/internal/core"
	"github.com/harsh3dev/messager/internal/queue"
	proto "github.com/harsh3dev/messager/proto/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the BrokerServer gRPC interface.
type Server struct {
	proto.UnimplementedBrokerServer
	manager *queue.Manager
}

func NewServer(manager *queue.Manager) *Server {
	return &Server{manager: manager}
}

func (s *Server) Publish(ctx context.Context, req *proto.PublishRequest) (*proto.PublishResponse, error) {
	messageID, err := newMessageID()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate id: %v", err)
	}

	headers := make([]core.Header, len(req.Headers))
	for i, h := range req.Headers {
		headers[i] = core.Header{Key: h.Key, Value: h.Value}
	}

	msg := core.Message{
		ID:          core.MessageID(messageID),
		Queue:       req.Queue,
		Payload:     req.Payload,
		Headers:     headers,
		EnqueueTime: time.Now(),
	}

	if err := s.manager.Enqueue(msg); err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue: %v", err)
	}

	return &proto.PublishResponse{MessageId: messageID}, nil
}

func (s *Server) Subscribe(stream grpc.BidiStreamingServer[proto.ConsumerMessage, proto.SubscribeEvent]) error {
	ctx := stream.Context()

	// First message must establish the subscription.
	firstMsg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "recv subscribe request: %v", err)
	}
	subscribeReq := firstMsg.GetSubscribe()
	if subscribeReq == nil {
		return status.Error(codes.InvalidArgument, "first message must be a SubscribeRequest")
	}
	queueName := subscribeReq.Queue

	done := make(chan error, 2)

	// Send goroutine: pull messages from the queue and push to the consumer.
	// May stay blocked in Dequeue after the consumer disconnects; it unblocks
	// when the Manager closes at server shutdown.
	go func() {
		for {
			msg, ok := s.manager.Dequeue(queueName)
			if !ok {
				done <- nil
				return
			}
			select {
			case <-ctx.Done():
				done <- nil
				return
			default:
			}
			if err := stream.Send(coreToProtoEvent(msg)); err != nil {
				done <- err
				return
			}
		}
	}()

	// Recv goroutine: read ACK/NACK messages from the consumer.
	go func() {
		for {
			consumerMsg, err := stream.Recv()
			if err != nil {
				if err == io.EOF || status.Code(err) == codes.Canceled {
					done <- nil
				} else {
					done <- err
				}
				return
			}
			if ack := consumerMsg.GetAck(); ack != nil {
				_ = ack // routed to Ack Manager in Phase 6
			}
		}
	}()

	return <-done
}

func coreToProtoEvent(msg core.Message) *proto.SubscribeEvent {
	headers := make([]*proto.Header, len(msg.Headers))
	for i, h := range msg.Headers {
		headers[i] = &proto.Header{Key: h.Key, Value: h.Value}
	}
	return &proto.SubscribeEvent{
		Event: &proto.SubscribeEvent_Message{
			Message: &proto.Message{
				Id:              string(msg.ID),
				Queue:           msg.Queue,
				Payload:         msg.Payload,
				Headers:         headers,
				RetryCount:      msg.RetryCount,
				EnqueueTimeUnix: msg.EnqueueTime.UnixNano(),
			},
		},
	}
}

func newMessageID() (string, error) {
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(randomBytes), nil
}
