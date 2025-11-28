package hive

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Block represents the portion of a Hive block we care about.
type Block struct {
	Number       int64         `json:"-"`
	Transactions []Transaction `json:"transactions"`
}

// Transaction wraps transaction operations.
type Transaction struct {
	Operations []Operation `json:"operations"`
}

// Operation is an op tuple of [name, payload].
type Operation struct {
	Type  string
	Value json.RawMessage
}

// UnmarshalJSON decodes the Hive operation tuple.
func (o *Operation) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) != 2 {
		return fmt.Errorf("unexpected op format: %s", string(data))
	}
	if err := json.Unmarshal(raw[0], &o.Type); err != nil {
		return err
	}
	o.Value = raw[1]
	return nil
}

// CustomJSONOp represents the custom_json operation payload.
type CustomJSONOp struct {
	ID                   string          `json:"id"`
	JSON                 json.RawMessage `json:"json"`
	RequiredAuths        []string        `json:"required_auths"`
	RequiredPostingAuths []string        `json:"required_posting_auths"`
}

// ExtractPayload flattens the optional nested JSON string into raw bytes.
func (c CustomJSONOp) ExtractPayload() ([]byte, error) {
	if len(c.JSON) == 0 {
		return nil, errors.New("missing json field")
	}

	// The payload is often encoded as a string containing JSON.
	var asString string
	if err := json.Unmarshal(c.JSON, &asString); err == nil {
		return []byte(asString), nil
	}

	// Otherwise assume it was already an object/array.
	return c.JSON, nil
}
