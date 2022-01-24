package commands

import (
	"encoding/hex"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Get struct {
	ReadOnly
	CommandBase
	request *kvrpcpb.GetRequest
}

func NewGet(request *kvrpcpb.GetRequest) Get {
	return Get{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.Version,
		},
		request: request,
	}
}

func (g *Get) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	key := g.request.Key
	log.Debug("read key", zap.Uint64("start_ts", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))
	response := new(kvrpcpb.GetResponse)

	// YOUR CODE HERE (lab2).
	// Check for locks and their visibilities.
	// Hint: Check the interfaces provided by `mvcc.RoTxn`.
	lock, err := txn.GetLock(key)
	if err != nil {
		return response, nil, err
	}
	// block if there is a previous lock
	if lock != nil && txn.StartTS > lock.Ts {
		// TODO block error
		response.Error = &kvrpcpb.KeyError{Locked: lock.Info(key)}
		return response, nil, err
	}

	// YOUR CODE HERE (lab2).
	// Search writes for a committed value, set results in the response.
	// Hint: Check the interfaces provided by `mvcc.RoTxn`.
	val, err := txn.GetValue(key)
	if err != nil {
		return response, nil, err
	}
	// TestGetEmpty4B
	if val == nil {
		response.NotFound = true
	}
	response.Value = val

	return response, nil, nil
}
