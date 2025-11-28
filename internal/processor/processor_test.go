package processor

import (
	"context"
	"encoding/json"
	"testing"

	"hivemoji/internal/hive"
	"hivemoji/internal/storage"
)

// recordingStore captures calls from Processor for assertions.
type recordingStore struct {
	lastV1    storage.RegisterV1
	lastBlock int64
}

func (r *recordingStore) UpsertV1(ctx context.Context, payload storage.RegisterV1) error {
	r.lastV1 = payload
	return nil
}

func (r *recordingStore) DeleteEmoji(ctx context.Context, author, name string) error { return nil }

func (r *recordingStore) SaveChunk(ctx context.Context, chunk storage.ChunkPayload) (*storage.AssembledSet, error) {
	return nil, nil
}

func (r *recordingStore) GetChunkSet(ctx context.Context, uploadID, kind string) (*storage.AssembledSet, error) {
	return nil, nil
}

func (r *recordingStore) UpsertFromChunks(ctx context.Context, main *storage.AssembledSet, fallback *storage.AssembledSet) error {
	return nil
}

func (r *recordingStore) SetLastBlock(ctx context.Context, number int64) error {
	r.lastBlock = number
	return nil
}

// TestProcessBlock_V1Register verifies author extraction and v1 register handling using block 101482212.
func TestProcessBlock_V1Register(t *testing.T) {
	store := &recordingStore{}
	proc := &Processor{store: store}

	payload := `{"op":"register","version":1,"name":"pained_laugh","mime":"image/webp","width":96,"height":96,"data":"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"}`

	opEnvelope := map[string]interface{}{
		"id":                     "hivemoji",
		"json":                   payload,
		"required_auths":         []string{},
		"required_posting_auths": []string{"mrtats"},
	}
	rawOp, err := json.Marshal(opEnvelope)
	if err != nil {
		t.Fatalf("marshal op envelope: %v", err)
	}

	opValue := hive.Operation{Type: "custom_json", Value: rawOp}

	block := &hive.Block{
		Number: 101482212,
		Transactions: []hive.Transaction{
			{Operations: []hive.Operation{opValue}},
		},
	}

	if err := proc.ProcessBlock(context.Background(), block); err != nil {
		t.Fatalf("ProcessBlock error: %v", err)
	}

	if store.lastV1.Author != "mrtats" {
		t.Fatalf("expected author mrtats, got %q", store.lastV1.Author)
	}
	if store.lastV1.Name != "pained_laugh" {
		t.Fatalf("expected name pained_laugh, got %s", store.lastV1.Name)
	}
	if store.lastBlock != 101482212 {
		t.Fatalf("expected last block 101482212, got %d", store.lastBlock)
	}
	if len(store.lastV1.Data) == 0 {
		t.Fatalf("expected image data to be present")
	}
}
