package hive

import (
	"context"
	"encoding/json"
	"fmt"

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
