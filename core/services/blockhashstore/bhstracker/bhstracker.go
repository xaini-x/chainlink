package bhstracker

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"math/big"
	"sync"

	"github.com/smartcontractkit/chainlink/core/logger"
)

// Store is an interface for triggering blockhash stores and checking whether blockhashes are
// already stored.
type Store interface {
	// Store the hash associated with blockNum.
	Store(ctx context.Context, blockNum uint64) error

	// IsStored checks whether the hash associated with blockNum is already stored.
	IsStored(ctx context.Context, blockNum uint64) (bool, error)
}

type Tracker struct {
	store Store

	logger logger.Logger

	// minBlockAge is the minimum age of blocks for which to store blockhashes.
	minBlockAge int

	// maxBlockAge is the maximum age of blocks for which to store blockhashes.
	maxBlockAge int

	// mu must be acquired while reading and writing blockToRequests, requestIDToBlock, and minBlock.
	mu sync.Mutex

	// blockToRequests maps the block number to the current request data for that block.
	blockToRequests map[uint64]*requests

	// requestIDToBlock maps the request ID to the block where the request was made.
	requestIDToBlock map[string]uint64
}

func NewTracker(store Store, minBlockAge int, maxBlockAge int, logger logger.Logger) *Tracker {
	return &Tracker{
		store:            store,
		logger:           logger,
		minBlockAge:      minBlockAge,
		maxBlockAge:      maxBlockAge,
		mu:               sync.Mutex{},
		blockToRequests:  make(map[uint64]*requests),
		requestIDToBlock: make(map[string]uint64),
	}
}

type requests struct {
	// unfulfilledRequest is a set of unfufilled request IDs represented as strings.
	unfulfilledRequests map[string]struct{}

	// callbacks that should be acked once the block corresponding to these requests has its hash
	// stored or falls out of scope.
	callbacks []func()
}

// Request adds the given request information to the tracker.
func (t *Tracker) Request(block uint64, requestID *big.Int, callback func()) {
	t.logger.Debugw("BHSTracker: Received new request", "requestID", requestID, "block", block)

	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.blockToRequests[block]; !ok {
		t.blockToRequests[block] = &requests{unfulfilledRequests: make(map[string]struct{})}
	}
	t.blockToRequests[block].unfulfilledRequests[requestID.String()] = struct{}{}
	t.blockToRequests[block].callbacks = append(t.blockToRequests[block].callbacks, callback)
	t.requestIDToBlock[requestID.String()] = block
}

// Fulfillment adds the given fulfillment information to the tracker.
func (t *Tracker) Fulfillment(requestID *big.Int, callback func()) {
	t.logger.Debugw("BHSTracker: Received new fulfillment", "requestID", requestID)

	t.mu.Lock()
	defer t.mu.Unlock()

	requestBlock, ok := t.requestIDToBlock[requestID.String()]
	if !ok {
		// This could happen if either:
		//  - This block has already been stored, and therefor the state has been discarded.
		//  - This block was not able to be stored for some reason, and the state was discarded due to being more than
		//    maxBlockAge blocks old.
		//  - The request was already more than maxBlockAge blocks old when this tracker started, and was therefor not
		//    stored.
		t.logger.Debugw("BHSTracker: Received fulfillment for an untracked request", "requestID", requestID)
		callback()
		return
	}

	requests, ok := t.blockToRequests[requestBlock]
	if !ok {
		// This should never happen; if the requestID is present in requestIDToBlock there should always be an entry in
		// blockToRequests for that block.
		t.logger.Errorw("BHSTracker: Expected to have state for block", "block", requestBlock, "requestID", requestID)
		callback()
		return
	}

	delete(requests.unfulfilledRequests, requestID.String())
	delete(t.requestIDToBlock, requestID.String())
}

// StoreHashes checks the state of the tracker to find any blockhashes that should be stored and stores them.
func (t *Tracker) StoreHashes(ctx context.Context, currentBlockNum uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var err error
	for block, requests := range t.blockToRequests {
		if block > currentBlockNum-uint64(t.minBlockAge) {
			// Block is too young to be eligible for storage.
			continue
		}

		if block < currentBlockNum-uint64(t.maxBlockAge) {
			// Block is too old to be eligible for storage. This indicates an issue since it implies the block was not
			// successfully processed while it was between minBlockAge and maxBlockAge blocks old.
			t.logger.Errorw("BHSTracker: Too late to store block",
				"block", block, "currentBlock", currentBlockNum)
			t.cleanupState(block, requests)
			err = multierr.Append(err, errors.Errorf("too late to store block: %d", block))
			continue
		}

		// Block is eligible for storage
		storeErr := t.maybeStoreBlock(ctx, block, requests)
		if err != nil {
			t.logger.Errorw("BHSTracker: Error storing block",
				"err", storeErr, "block", block, "currentBlock", currentBlockNum)
			err = multierr.Append(err, storeErr)
		}
	}
	return err
}

func (t *Tracker) maybeStoreBlock(ctx context.Context, block uint64, requests *requests) error {
	if len(requests.unfulfilledRequests) > 0 {
		stored, err := t.store.IsStored(ctx, block)
		if err != nil {
			return fmt.Errorf("checking if block %d is already stored: %w", block, err)
		}

		if !stored {
			err := t.store.Store(ctx, block)
			if err != nil {
				return fmt.Errorf("storing block %d: %w", block, err)
			}
		}
	}
	t.cleanupState(block, requests)
	return nil
}

func (t *Tracker) cleanupState(block uint64, requests *requests) {
	for reqID := range requests.unfulfilledRequests {
		delete(t.requestIDToBlock, reqID)
	}
	for _, callback := range requests.callbacks {
		callback()
	}
	delete(t.blockToRequests, block)
}
