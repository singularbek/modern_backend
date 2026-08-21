package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"

	catalogpb "go-catalog/catalog/gen" // Generated code from protoc

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
)

// 1. STRUCT IMPLEMENTATION
type server struct {
	catalogpb.UnimplementedCatalogServiceServer
	kafkaWriter *kafka.Writer
}

// 2. THE RPC METHOD
func (s *server) CreateDraft(ctx context.Context, req *catalogpb.CreateDraftRequest) (*catalogpb.CreateDraftResponse, error) {
	// Construct a unique draft ID
	draftID := fmt.Sprintf("drft_%s_123", req.GetMerchantId())

	// Build the event payload to publish to Kafka
	payload := map[string]interface{}{
		"event_type":    "PRODUCT_DRAFT_STAGED",
		"draft_id":      draftID,
		"merchant_id":   req.GetMerchantId(),
		"title":         req.GetTitle(),
		"price_etb":     req.GetPriceEtb(),
		"initial_stock": req.GetInitialStock(),
	}
	bytes, _ := json.Marshal(payload)

	// 3. PUBLISH TO KAFKA
	err := s.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(req.GetMerchantId()), // Partition key ensures ordering per merchant
		Value: bytes,
	})
	if err != nil {
		log.Printf("Failed to write to Kafka: %v", err)
	} else {
		log.Printf("[Go Service] Published event to Kafka topic 'commerce.catalog.v1' for draft %s", draftID)
	}

	// 4. RETURN RESPONSE TO TS BFF
	return &catalogpb.CreateDraftResponse{
		DraftId: draftID,
		Status:  "STAGED_SUCCESSFULLY",
	}, nil
}

// 5. SERVER BOOTSTRAP
func main() {
	// Initialize the Kafka Producer
	writer := &kafka.Writer{
		Addr:     kafka.TCP("127.0.0.1:9092"),
		Topic:    "commerce.catalog.v1",
		Balancer: &kafka.LeastBytes{},
	}

	// Create TCP network listener on port 50051
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Register our server struct with the gRPC framework
	s := grpc.NewServer()
	catalogpb.RegisterCatalogServiceServer(s, &server{kafkaWriter: writer})

	log.Println("[Go Service] Listening on gRPC port 50051...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
