package hive

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	hivego "github.com/deathwingtheboss/hivego"
	"github.com/deathwingtheboss/hivego/types"
)

// Client wraps hivego RPC calls to a Hive node.
type Client struct {
	node *hivego.HiveRpcNode
}

// NewClient builds a Hive RPC client using the given endpoint.
func NewClient(baseURL string) *Client {
	return &Client{
		node: hivego.NewHiveRpc(baseURL),
	}
}

// GetBlock fetches a block by number. It returns (nil, nil) when the node has not produced the block yet.
func (c *Client) GetBlock(ctx context.Context, number int64) (*Block, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	raw, err := c.node.GetBlock(int(number))
	if err != nil {
		return nil, fmt.Errorf("get block %d: %w", number, err)
	}

	if raw.BlockID == "" {
		// Not yet produced.
		return nil, nil
	}

	block := Block{
		Number:       int64(raw.BlockNumber),
		Transactions: make([]Transaction, 0, len(raw.Transactions)),
	}
	if block.Number == 0 {
		block.Number = number
	}

	for _, tx := range raw.Transactions {
		t := Transaction{}
		for _, op := range tx.Operations {
			if op.Type == "" {
				continue
			}

			payload, err := json.Marshal(op.Value)
			if err != nil {
				return nil, fmt.Errorf("marshal op %s: %w", op.Type, err)
			}

			opType := op.Type
			if opType == types.OperationType.CustomJson {
				opType = "custom_json"
			}

			t.Operations = append(t.Operations, Operation{
				Type:  opType,
				Value: payload,
			})
		}
		block.Transactions = append(block.Transactions, t)
	}

	return &block, nil
}

// HeadBlockNumber fetches the chain head block number.
func (c *Client) HeadBlockNumber(ctx context.Context) (int64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	raw, err := c.node.GetDynamicGlobalProps()
	if err != nil {
		return 0, fmt.Errorf("head block props: %w", err)
	}

	var props map[string]json.RawMessage
	if err := json.Unmarshal(raw, &props); err != nil {
		return 0, fmt.Errorf("decode global props: %w", err)
	}

	var head any
	if v, ok := props["head_block_number"]; ok {
		head = v
	} else {
		return 0, fmt.Errorf("head_block_number missing in global props")
	}

	var n int64
	switch t := head.(type) {
	case json.RawMessage:
		if err := json.Unmarshal(t, &n); err == nil {
			return n, nil
		}
		var s string
		if err := json.Unmarshal(t, &s); err == nil {
			i, err := strconv.ParseInt(s, 10, 64)
			if err == nil {
				return i, nil
			}
		}
	case float64:
		n = int64(t)
		return n, nil
	}

	return 0, fmt.Errorf("unable to parse head_block_number")
}
