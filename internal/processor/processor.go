package processor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"hivemoji/internal/hive"
	"hivemoji/internal/storage"
)

// Processor orchestrates Hive block processing into storage.
type Processor struct {
	store  store
	client *hive.Client
}

// store defines the methods Processor needs from storage.Store.
type store interface {
	UpsertV1(ctx context.Context, payload storage.RegisterV1) error
	DeleteEmoji(ctx context.Context, author, name string) error
	SaveChunk(ctx context.Context, chunk storage.ChunkPayload) (*storage.AssembledSet, error)
	GetChunkSet(ctx context.Context, uploadID, kind string) (*storage.AssembledSet, error)
	UpsertFromChunks(ctx context.Context, main *storage.AssembledSet, fallback *storage.AssembledSet) error
	SetLastBlock(ctx context.Context, number int64) error
}

// New builds a Processor.
func New(store *storage.Store, client *hive.Client) *Processor {
	return &Processor{store: store, client: client}
}

// ProcessBlock scans a block for hivemoji custom_json entries.
func (p *Processor) ProcessBlock(ctx context.Context, block *hive.Block) error {
	for _, tx := range block.Transactions {
		for _, op := range tx.Operations {
			if op.Type != "custom_json" {
				continue
			}

			var custom hive.CustomJSONOp
			if err := json.Unmarshal(op.Value, &custom); err != nil {
				log.Printf("skip custom_json decode error: %v", err)
				continue
			}
			if custom.ID != "hivemoji" {
				continue
			}

			payloadBytes, err := custom.ExtractPayload()
			if err != nil {
				log.Printf("invalid hivemoji payload: %v", err)
				continue
			}

			author := firstNonEmpty(custom.RequiredPostingAuths, custom.RequiredAuths)

			if err := p.handlePayload(ctx, block.Number, payloadBytes, author); err != nil {
				return fmt.Errorf("block %d: %w", block.Number, err)
			}
		}
	}

	if err := p.store.SetLastBlock(ctx, block.Number); err != nil {
		return err
	}
	return nil
}

func (p *Processor) handlePayload(ctx context.Context, blockNum int64, payload []byte, author string) error {
	var env struct {
		Version int    `json:"version"`
		Op      string `json:"op"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		return fmt.Errorf("payload envelope: %w", err)
	}

	log.Printf("block %d: hivemoji v%d op=%s author=%s", blockNum, env.Version, env.Op, safeAuthor(author))

	switch env.Version {
	case 1:
		return p.handleV1(ctx, blockNum, payload, author)
	case 2:
		return p.handleV2(ctx, blockNum, payload, author)
	default:
		return fmt.Errorf("unsupported version %d", env.Version)
	}
}

func (p *Processor) handleV1(ctx context.Context, blockNum int64, payload []byte, author string) error {
	var msg struct {
		Version  int             `json:"version"`
		Op       string          `json:"op"`
		Name     string          `json:"name"`
		Mime     string          `json:"mime"`
		Width    int             `json:"width"`
		Height   int             `json:"height"`
		Data     string          `json:"data"`
		Animated bool            `json:"animated"`
		Loop     json.RawMessage `json:"loop"`
		Fallback *struct {
			Mime string `json:"mime"`
			Data string `json:"data"`
		} `json:"fallback"`
	}

	if err := json.Unmarshal(payload, &msg); err != nil {
		return fmt.Errorf("decode v1: %w", err)
	}

	switch msg.Op {
	case "register":
		loop, err := parseLoop(msg.Loop)
		if err != nil {
			return fmt.Errorf("loop: %w", err)
		}
		raw, err := base64.StdEncoding.DecodeString(msg.Data)
		if err != nil {
			return fmt.Errorf("decode v1 data: %w", err)
		}
		var fallbackData []byte
		var fallbackMime string
		if msg.Fallback != nil {
			fb, err := base64.StdEncoding.DecodeString(msg.Fallback.Data)
			if err != nil {
				return fmt.Errorf("decode fallback: %w", err)
			}
			fallbackData = fb
			fallbackMime = msg.Fallback.Mime
		}

		log.Printf(
			"block %d: v1 register name=%s author=%s animated=%t loop=%v bytes=%d fallback_bytes=%d",
			blockNum,
			msg.Name,
			safeAuthor(author),
			msg.Animated,
			loop,
			len(raw),
			len(fallbackData),
		)

		return p.store.UpsertV1(ctx, storage.RegisterV1{
			Name:         msg.Name,
			Author:       author,
			Mime:         msg.Mime,
			Width:        msg.Width,
			Height:       msg.Height,
			Data:         raw,
			Animated:     msg.Animated,
			Loop:         loop,
			FallbackMime: fallbackMime,
			FallbackData: fallbackData,
		})

	case "delete":
		return p.store.DeleteEmoji(ctx, author, msg.Name)
	default:
		return fmt.Errorf("unknown v1 op %q", msg.Op)
	}
}

func (p *Processor) handleV2(ctx context.Context, blockNum int64, payload []byte, author string) error {
	var msg struct {
		Version  int             `json:"version"`
		Op       string          `json:"op"`
		ID       string          `json:"id"`
		Name     string          `json:"name"`
		Mime     string          `json:"mime"`
		Width    int             `json:"width"`
		Height   int             `json:"height"`
		Animated bool            `json:"animated"`
		Loop     json.RawMessage `json:"loop"`
		Checksum string          `json:"checksum"`
		Kind     string          `json:"kind"`
		Seq      int             `json:"seq"`
		Total    int             `json:"total"`
		Data     string          `json:"data"`
	}

	if err := json.Unmarshal(payload, &msg); err != nil {
		return fmt.Errorf("decode v2: %w", err)
	}

	if msg.Op != "chunk" && msg.Op != "register" && msg.Op != "" {
		return fmt.Errorf("unsupported v2 op %q", msg.Op)
	}

	if msg.Op == "register" && msg.Data == "" {
		// Manifest-only entry for discovery; nothing to persist.
		log.Printf(
			"block %d: v2 register manifest name=%s author=%s upload=%s animated=%t loop=%s",
			blockNum,
			msg.Name,
			safeAuthor(author),
			msg.ID,
			msg.Animated,
			string(msg.Loop),
		)
		return nil
	}

	if msg.Total <= 0 {
		return errors.New("total must be > 0")
	}

	if msg.Seq <= 0 {
		log.Printf(
			"block %d: skip v2 chunk upload=%s kind=%s name=%s seq=%d (must be > 0)",
			blockNum,
			msg.ID,
			msg.Kind,
			msg.Name,
			msg.Seq,
		)
		return nil
	}

	loop, err := parseLoop(msg.Loop)
	if err != nil {
		return fmt.Errorf("loop: %w", err)
	}

	data, err := base64.StdEncoding.DecodeString(msg.Data)
	if err != nil {
		return fmt.Errorf("decode v2 chunk: %w", err)
	}

	kind := msg.Kind
	if kind == "" {
		kind = "main"
	}

	assembled, err := p.store.SaveChunk(ctx, storage.ChunkPayload{
		ID:       msg.ID,
		Author:   author,
		Name:     msg.Name,
		Version:  msg.Version,
		Mime:     msg.Mime,
		Width:    msg.Width,
		Height:   msg.Height,
		Animated: msg.Animated,
		Loop:     loop,
		Checksum: msg.Checksum,
		Kind:     kind,
		Seq:      msg.Seq,
		Total:    msg.Total,
		Data:     data,
	})
	if err != nil {
		return err
	}

	if assembled == nil {
		return nil
	}

	log.Printf(
		"block %d: v2 assembled upload=%s kind=%s name=%s author=%s animated=%t loop=%v bytes=%d",
		blockNum,
		assembled.UploadID,
		assembled.Kind,
		assembled.Name,
		safeAuthor(assembled.Author),
		assembled.Animated,
		assembled.Loop,
		len(assembled.Data),
	)

	return p.handleCompletedSet(ctx, assembled)
}

func (p *Processor) handleCompletedSet(ctx context.Context, set *storage.AssembledSet) error {
	switch set.Kind {
	case "main":
		fallback, err := p.store.GetChunkSet(ctx, set.UploadID, "fallback")
		if err != nil {
			return err
		}
		return p.store.UpsertFromChunks(ctx, set, fallback)
	case "fallback":
		mainSet, err := p.store.GetChunkSet(ctx, set.UploadID, "main")
		if err != nil {
			return err
		}
		if mainSet == nil {
			// Fallback arrived before main; do nothing until main completes.
			return nil
		}
		return p.store.UpsertFromChunks(ctx, mainSet, set)
	default:
		return fmt.Errorf("unknown chunk kind %q", set.Kind)
	}
}

// FetchBlock wraps the Hive client to retrieve a block.
func (p *Processor) FetchBlock(ctx context.Context, number int64) (*hive.Block, error) {
	return p.client.GetBlock(ctx, number)
}

// HeadBlockNumber returns the chain head block number from the Hive node.
func (p *Processor) HeadBlockNumber(ctx context.Context) (int64, error) {
	return p.client.HeadBlockNumber(ctx)
}

func firstNonEmpty(primary []string, fallback []string) string {
	if len(primary) > 0 && primary[0] != "" {
		return primary[0]
	}
	if len(fallback) > 0 {
		return fallback[0]
	}
	return ""
}

func safeAuthor(author string) string {
	if strings.TrimSpace(author) == "" {
		return "<unknown>"
	}
	return author
}

// parseLoop accepts either an int or a boolean for the loop field.
// Booleans map to nil/zero to keep storage typed as *int.
func parseLoop(raw json.RawMessage) (*int, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}

	var asBool bool
	if err := json.Unmarshal(raw, &asBool); err == nil {
		if !asBool {
			return nil, nil
		}
		zero := 0
		return &zero, nil
	}

	var asInt int
	if err := json.Unmarshal(raw, &asInt); err == nil {
		return &asInt, nil
	}

	return nil, fmt.Errorf("loop must be boolean or integer")
}
