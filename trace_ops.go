package iavl

import tmcrypto "github.com/tendermint/tendermint/proto/tendermint/crypto"

const (
	WriteOp  Operation = "write"
	ReadOp   Operation = "read"
	DeleteOp Operation = "delete"
)

type (
	// operation represents an IO operation
	Operation string

	// traceOperation implements a traced KVStore operation
	TraceOperation struct {
		Operation Operation `json:"operation"`
		Key       string    `json:"key"`
		Value     string    `json:"value"`
	}
)

// Witness data represents a trace operation along with inclusion proofs required for said operation
type WitnessData struct {
	Operation Operation
	Key       []byte
	Value     []byte
	Proofs    []tmcrypto.ProofOp
}
