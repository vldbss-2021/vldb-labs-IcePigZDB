package commands

import (
	"encoding/hex"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ResolveLock struct {
	CommandBase
	request  *kvrpcpb.ResolveLockRequest
	keyLocks []mvcc.KlPair
}

func NewResolveLock(request *kvrpcpb.ResolveLockRequest) ResolveLock {
	return ResolveLock{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.StartVersion,
		},
		request: request,
	}
}

func (rl *ResolveLock) PrepareWrites(txn *mvcc.MvccTxn) (interface{}, error) {
	// A map from start timestamps to commit timestamps which tells us whether a transaction (identified by start ts)
	// has been committed (and if so, then its commit ts) or rolled back (in which case the commit ts is 0).
	commitTs := rl.request.CommitVersion
	response := new(kvrpcpb.ResolveLockResponse)

	log.Info("There keys to resolve",
		zap.Uint64("lockTS", txn.StartTS),
		zap.Int("number", len(rl.keyLocks)),
		zap.Uint64("commit_ts", commitTs))
	var commitKeys, rollbackKeys [][]byte
	for _, kl := range rl.keyLocks {
		// YOUR CODE HERE (lab2).
		// Try to commit the key if the transaction is committed already, or try to rollback the key if it's not.
		// The `commitKey` and `rollbackKey` functions could be useful.
		if commitTs > 0 {
			commitKeys = append(commitKeys, kl.Key)
		} else {
			rollbackKeys = append(rollbackKeys, kl.Key)
		}
		log.Debug("resolve key", zap.String("key", hex.EncodeToString(kl.Key)))
	}

	if len(commitKeys) > 0 {
		commiter := NewCommit(
			&kvrpcpb.CommitRequest{
				Context:       rl.request.Context,
				StartVersion:  txn.StartTS,
				Keys:          commitKeys,
				CommitVersion: commitTs,
			})
		commitResp, err := commiter.PrepareWrites(txn)
		if err != nil {
			return response, err
		}
		response.Error = commitResp.(*kvrpcpb.CommitResponse).Error
	}

	if len(rollbackKeys) > 0 {
		rollbacker := NewRollback(
			&kvrpcpb.BatchRollbackRequest{
				Context:      rl.request.Context,
				StartVersion: rl.request.StartVersion,
				Keys:         rollbackKeys,
			})
		rollbackResp, err := rollbacker.PrepareWrites(txn)
		if err != nil {
			return response, nil
		}
		response.Error = rollbackResp.(*kvrpcpb.BatchRollbackResponse).Error
	}

	return response, nil
}

func (rl *ResolveLock) WillWrite() [][]byte {
	return nil
}

func (rl *ResolveLock) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	// Find all locks where the lock's transaction (start ts) is in txnStatus.
	txn.StartTS = rl.request.StartVersion
	keyLocks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, nil, err
	}
	rl.keyLocks = keyLocks
	keys := [][]byte{}
	for _, kl := range keyLocks {
		keys = append(keys, kl.Key)
	}
	return nil, keys, nil
}
