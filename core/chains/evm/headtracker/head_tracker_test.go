package headtracker_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	gethCommon "github.com/ethereum/go-ethereum/common"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gopkg.in/guregu/null.v4"

	evmclient "github.com/smartcontractkit/chainlink/core/chains/evm/client"
	evmconfig "github.com/smartcontractkit/chainlink/core/chains/evm/config"
	"github.com/smartcontractkit/chainlink/core/chains/evm/headtracker"
	htmocks "github.com/smartcontractkit/chainlink/core/chains/evm/headtracker/mocks"
	httypes "github.com/smartcontractkit/chainlink/core/chains/evm/headtracker/types"
	evmtypes "github.com/smartcontractkit/chainlink/core/chains/evm/types"
	"github.com/smartcontractkit/chainlink/core/internal/cltest"
	"github.com/smartcontractkit/chainlink/core/internal/cltest/heavyweight"
	"github.com/smartcontractkit/chainlink/core/internal/testutils/configtest"
	"github.com/smartcontractkit/chainlink/core/internal/testutils/evmtest"
	"github.com/smartcontractkit/chainlink/core/internal/testutils/pgtest"
	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/utils"
	"github.com/smartcontractkit/sqlx"
)

func firstHead(t *testing.T, db *sqlx.DB) (h evmtypes.Head) {
	if err := db.Get(&h, `SELECT * FROM heads ORDER BY number ASC LIMIT 1`); err != nil {
		t.Fatal(err)
	}
	return h
}

func newCfg(t testing.TB) evmconfig.ChainScopedConfig {
	cfg := cltest.NewTestChainScopedConfig(t)
	return cfg
}

func TestHeadTracker_New(t *testing.T) {
	t.Parallel()

	db := pgtest.NewSqlxDB(t)
	logger := logger.TestLogger(t)
	config := cltest.NewTestGeneralConfig(t)

	ethClient, sub := cltest.NewEthClientAndSubMockWithDefaultChain(t)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(sub, nil)
	ethClient.On("HeadByNumber", mock.Anything, (*big.Int)(nil)).Return(cltest.Head(0), nil)
	sub.On("Unsubscribe").Maybe().Return(nil)
	sub.On("Err").Return(nil)

	orm := headtracker.NewORM(db, logger, config, cltest.FixtureChainID)
	assert.Nil(t, orm.IdempotentInsertHead(context.TODO(), cltest.Head(1)))
	last := cltest.Head(16)
	assert.Nil(t, orm.IdempotentInsertHead(context.TODO(), last))
	assert.Nil(t, orm.IdempotentInsertHead(context.TODO(), cltest.Head(10)))

	evmcfg := newCfg(t)
	ht := createHeadTracker(t, ethClient, evmcfg, orm)
	ht.Start(t)
	latest := ht.headSaver.LatestChain()
	require.NotNil(t, latest)
	assert.Equal(t, last.Number, latest.Number)
}

func TestHeadTracker_Save_InsertsAndTrimsTable(t *testing.T) {
	t.Parallel()

	db := pgtest.NewSqlxDB(t)
	logger := logger.TestLogger(t)
	config := newCfg(t)

	ethClient := cltest.NewEthClientMockWithDefaultChain(t)
	orm := headtracker.NewORM(db, logger, config, cltest.FixtureChainID)

	for idx := 0; idx < 200; idx++ {
		assert.Nil(t, orm.IdempotentInsertHead(context.TODO(), cltest.Head(idx)))
	}

	ht := createHeadTracker(t, ethClient, config, orm)

	h := cltest.Head(200)
	require.NoError(t, ht.headSaver.Save(context.TODO(), h))
	assert.Equal(t, big.NewInt(200), ht.headSaver.LatestChain().ToInt())

	firstHead := firstHead(t, db)
	assert.Equal(t, big.NewInt(101), firstHead.ToInt())

	lastHead, err := orm.LatestHead(context.TODO())
	require.NoError(t, err)
	assert.Equal(t, int64(200), lastHead.Number)
}

func TestHeadTracker_Get(t *testing.T) {
	t.Parallel()

	start := cltest.Head(5)

	tests := []struct {
		name    string
		initial *evmtypes.Head
		toSave  *evmtypes.Head
		want    *big.Int
	}{
		{"greater", start, cltest.Head(6), big.NewInt(6)},
		{"less than", start, cltest.Head(1), big.NewInt(5)},
		{"zero", start, cltest.Head(0), big.NewInt(5)},
		{"nil", start, nil, big.NewInt(5)},
		{"nil no initial", nil, nil, big.NewInt(0)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := pgtest.NewSqlxDB(t)
			logger := logger.TestLogger(t)
			config := newCfg(t)
			orm := headtracker.NewORM(db, logger, config, cltest.FixtureChainID)

			ethClient, sub := cltest.NewEthClientAndSubMockWithDefaultChain(t)
			sub.On("Err").Return(nil)
			sub.On("Unsubscribe").Return(nil)
			chStarted := make(chan struct{})
			ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
				Run(func(mock.Arguments) { close(chStarted) }).
				Return(sub, nil)
			ethClient.On("HeadByNumber", mock.Anything, (*big.Int)(nil)).Return(cltest.Head(0), nil)

			fnCall := ethClient.On("HeadByNumber", mock.Anything, mock.Anything)
			fnCall.RunFn = func(args mock.Arguments) {
				num := args.Get(1).(*big.Int)
				fnCall.ReturnArguments = mock.Arguments{cltest.Head(num.Int64()), nil}
			}

			if test.initial != nil {
				assert.Nil(t, orm.IdempotentInsertHead(context.TODO(), test.initial))
			}

			ht := createHeadTracker(t, ethClient, config, orm)
			ht.Start(t)

			if test.toSave != nil {
				err := ht.headSaver.Save(context.TODO(), test.toSave)
				assert.NoError(t, err)
			}

			assert.Equal(t, test.want, ht.headSaver.LatestChain().ToInt())
		})
	}
}

func TestHeadTracker_Start_NewHeads(t *testing.T) {
	t.Parallel()

	db := pgtest.NewSqlxDB(t)
	logger := logger.TestLogger(t)
	config := newCfg(t)
	orm := headtracker.NewORM(db, logger, config, cltest.FixtureChainID)

	ethClient, sub := cltest.NewEthClientAndSubMockWithDefaultChain(t)
	sub.On("Err").Return(nil)
	sub.On("Unsubscribe").Return(nil)
	chStarted := make(chan struct{})
	ethClient.On("HeadByNumber", mock.Anything, (*big.Int)(nil)).Return(cltest.Head(0), nil)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { close(chStarted) }).
		Return(sub, nil)

	ht := createHeadTracker(t, ethClient, config, orm)
	ht.Start(t)

	<-chStarted

	ethClient.AssertExpectations(t)
}

func TestHeadTracker_CallsHeadTrackableCallbacks(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	db := pgtest.NewSqlxDB(t)
	logger := logger.TestLogger(t)
	config := newCfg(t)
	orm := headtracker.NewORM(db, logger, config, cltest.FixtureChainID)

	ethClient, sub := cltest.NewEthClientAndSubMockWithDefaultChain(t)

	chchHeaders := make(chan chan<- *evmtypes.Head, 1)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			chchHeaders <- args.Get(1).(chan<- *evmtypes.Head)
		}).
		Return(sub, nil)
	ethClient.On("HeadByNumber", mock.Anything, mock.Anything).Return(cltest.Head(0), nil)

	sub.On("Unsubscribe").Return()
	sub.On("Err").Return(nil)

	checker := &cltest.MockHeadTrackable{}
	ht := createHeadTrackerWithChecker(t, ethClient, config, orm, checker)

	ht.Start(t)
	assert.Equal(t, int32(0), checker.OnNewLongestChainCount())

	headers := <-chchHeaders
	headers <- &evmtypes.Head{Number: 1, Hash: utils.NewHash(), EVMChainID: utils.NewBig(&cltest.FixtureChainID)}
	g.Eventually(func() int32 { return checker.OnNewLongestChainCount() }).Should(gomega.Equal(int32(1)))

	ht.Stop(t)
	assert.Equal(t, int32(1), checker.OnNewLongestChainCount())
}

func TestHeadTracker_ReconnectOnError(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	db := pgtest.NewSqlxDB(t)
	logger := logger.TestLogger(t)
	config := newCfg(t)
	orm := headtracker.NewORM(db, logger, config, cltest.FixtureChainID)

	ethClient, sub := cltest.NewEthClientAndSubMockWithDefaultChain(t)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(sub, nil)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(nil, errors.New("cannot reconnect"))
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(sub, nil)
	ethClient.On("HeadByNumber", mock.Anything, (*big.Int)(nil)).Return(cltest.Head(0), nil)
	chErr := make(chan error)
	sub.On("Unsubscribe").Return()
	sub.On("Err").Return((<-chan error)(chErr))

	checker := &cltest.MockHeadTrackable{}
	ht := createHeadTrackerWithChecker(t, ethClient, config, orm, checker)

	// connect
	ht.Start(t)
	assert.Equal(t, int32(0), checker.OnNewLongestChainCount())

	// trigger reconnect loop
	chErr <- errors.New("test error to force reconnect")
	g.Eventually(func() int32 {
		return checker.OnNewLongestChainCount()
	}).Should(gomega.Equal(int32(1)))
}

func TestHeadTracker_ResubscribeOnSubscriptionError(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)

	db := pgtest.NewSqlxDB(t)
	logger := logger.TestLogger(t)
	config := newCfg(t)
	orm := headtracker.NewORM(db, logger, config, cltest.FixtureChainID)

	ethClient, sub := cltest.NewEthClientAndSubMockWithDefaultChain(t)

	chchHeaders := make(chan chan<- *evmtypes.Head, 1)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { chchHeaders <- args.Get(1).(chan<- *evmtypes.Head) }).
		Twice().
		Return(sub, nil)
	ethClient.On("HeadByNumber", mock.Anything, mock.Anything).Return(cltest.Head(0), nil)

	sub.On("Unsubscribe").Return()
	sub.On("Err").Return(nil)

	checker := &cltest.MockHeadTrackable{}
	ht := createHeadTrackerWithChecker(t, ethClient, config, orm, checker)

	ht.Start(t)
	assert.Equal(t, int32(0), checker.OnNewLongestChainCount())

	headers := <-chchHeaders
	go func() {
		headers <- cltest.Head(1)
	}()

	g.Eventually(func() bool { return ht.headTracker.Healthy() == nil }, 5*time.Second, 5*time.Millisecond).Should(gomega.Equal(true))

	// trigger reconnect loop
	close(headers)

	// wait for full disconnect and a new subscription
	g.Eventually(func() int32 { return checker.OnNewLongestChainCount() }, 5*time.Second, 5*time.Millisecond).Should(gomega.Equal(int32(1)))
}

func TestHeadTracker_Start_LoadsLatestChain(t *testing.T) {
	t.Parallel()

	db := pgtest.NewSqlxDB(t)
	logger := logger.TestLogger(t)
	config := newCfg(t)
	ethClient, sub := cltest.NewEthClientAndSubMockWithDefaultChain(t)

	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).Return(sub, nil)

	heads := []*evmtypes.Head{
		cltest.Head(0),
		cltest.Head(1),
		cltest.Head(2),
		cltest.Head(3),
	}
	var parentHash gethCommon.Hash
	for i := 0; i < len(heads); i++ {
		if parentHash != (gethCommon.Hash{}) {
			heads[i].ParentHash = parentHash
		}
		parentHash = heads[i].Hash
	}
	ethClient.On("HeadByNumber", mock.Anything, (*big.Int)(nil)).Return(heads[3], nil)
	ethClient.On("HeadByNumber", mock.Anything, big.NewInt(2)).Return(heads[2], nil)
	ethClient.On("HeadByNumber", mock.Anything, big.NewInt(1)).Return(heads[1], nil)
	ethClient.On("HeadByNumber", mock.Anything, big.NewInt(0)).Return(heads[0], nil)

	sub.On("Unsubscribe").Return()
	sub.On("Err").Return(nil)

	orm := headtracker.NewORM(db, logger, config, cltest.FixtureChainID)
	trackable := new(htmocks.HeadTrackable)
	trackable.Test(t)
	ht := createHeadTrackerWithChecker(t, ethClient, config, orm, trackable)

	require.NoError(t, orm.IdempotentInsertHead(context.Background(), heads[2]))

	trackable.On("Connect", mock.Anything).Return(nil)
	trackable.On("OnNewLongestChain", mock.Anything, mock.MatchedBy(func(h evmtypes.Head) bool {
		return h.Number == 3 && h.Hash == heads[3].Hash && h.ParentHash == heads[2].Hash && h.Parent.Number == 2 && h.Parent.Hash == heads[2].Hash && h.Parent.Parent == nil
	})).Once().Return()
	ht.Start(t)

	h, err := orm.LatestHead(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, h)
	assert.Equal(t, h.Number, int64(3))
}

func TestHeadTracker_SwitchesToLongestChainWithHeadSamplingEnabled(t *testing.T) {
	// Need separate db because ht.Stop() will cancel the ctx, causing a db connection
	// close and go-txdb rollback.
	config, db := heavyweight.FullTestDB(t, "switches_longest_chain", true, true)
	config.Overrides.GlobalEvmFinalityDepth = null.IntFrom(50)
	// Need to set the buffer to something large since we inject a lot of heads at once and otherwise they will be dropped
	config.Overrides.GlobalEvmHeadTrackerMaxBufferSize = null.IntFrom(42)

	// Head sampling enabled
	d := 2500 * time.Millisecond
	config.Overrides.GlobalEvmHeadTrackerSamplingInterval = &d

	ethClient, sub := cltest.NewEthClientAndSubMockWithDefaultChain(t)

	checker := new(htmocks.HeadTrackable)
	checker.Test(t)
	orm := headtracker.NewORM(db, logger.TestLogger(t), config, *config.DefaultChainID())
	ht := createHeadTrackerWithChecker(t, ethClient, evmtest.NewChainScopedConfig(t, config), orm, checker)

	chchHeaders := make(chan chan<- *evmtypes.Head, 1)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { chchHeaders <- args.Get(1).(chan<- *evmtypes.Head) }).
		Return(sub, nil)
	sub.On("Unsubscribe").Return()
	sub.On("Err").Return(nil)

	// ---------------------
	blocks := cltest.NewBlocks(t, 10)

	head0 := blocks.Head(0)
	// Initial query
	ethClient.On("HeadByNumber", mock.Anything, (*big.Int)(nil)).Return(head0, nil)
	ht.Start(t)

	headSeq := cltest.NewHeadBuffer(t)
	headSeq.Append(blocks.Head(0))
	headSeq.Append(blocks.Head(1))

	// Blocks 2 and 3 are out of order
	headSeq.Append(blocks.Head(3))
	headSeq.Append(blocks.Head(2))

	// Block 4 comes in
	headSeq.Append(blocks.Head(4))

	// Another block at level 4 comes in, that will be uncled
	headSeq.Append(blocks.NewHead(4))

	// Reorg happened forking from block 2
	blocksForked := blocks.ForkAt(t, 2, 5)
	headSeq.Append(blocksForked.Head(2))
	headSeq.Append(blocksForked.Head(3))
	headSeq.Append(blocksForked.Head(4))
	headSeq.Append(blocksForked.Head(5)) // Now the new chain is longer

	lastLongestChainAwaiter := cltest.NewAwaiter()

	// the callback is only called for head number 5 because of head sampling
	checker.On("OnNewLongestChain", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			h := args.Get(1).(*evmtypes.Head)

			assert.Equal(t, int64(5), h.Number)
			assert.Equal(t, blocksForked.Head(5).Hash, h.Hash)

			// This is the new longest chain, check that it came with its parents
			if !assert.NotNil(t, h.Parent) {
				return
			}
			assert.Equal(t, h.Parent.Hash, blocksForked.Head(4).Hash)
			if !assert.NotNil(t, h.Parent.Parent) {
				return
			}
			assert.Equal(t, h.Parent.Parent.Hash, blocksForked.Head(3).Hash)
			if !assert.NotNil(t, h.Parent.Parent.Parent) {
				return
			}
			assert.Equal(t, h.Parent.Parent.Parent.Hash, blocksForked.Head(2).Hash)
			if !assert.NotNil(t, h.Parent.Parent.Parent.Parent) {
				return
			}
			assert.Equal(t, h.Parent.Parent.Parent.Parent.Hash, blocksForked.Head(1).Hash)
			lastLongestChainAwaiter.ItHappened()
		}).Return().Once()

	headers := <-chchHeaders

	// This grotesque construction is the only way to do dynamic return values using
	// the mock package.  We need dynamic returns because we're simulating reorgs.
	latestHeadByNumber := make(map[int64]*evmtypes.Head)
	latestHeadByNumberMu := new(sync.Mutex)

	fnCall := ethClient.On("HeadByNumber", mock.Anything, mock.Anything)
	fnCall.RunFn = func(args mock.Arguments) {
		latestHeadByNumberMu.Lock()
		defer latestHeadByNumberMu.Unlock()
		num := args.Get(1).(*big.Int)
		head, exists := latestHeadByNumber[num.Int64()]
		if !exists {
			head = cltest.Head(num.Int64())
			latestHeadByNumber[num.Int64()] = head
		}
		fnCall.ReturnArguments = mock.Arguments{head, nil}
	}

	for _, h := range headSeq.Heads {
		latestHeadByNumberMu.Lock()
		latestHeadByNumber[h.Number] = h
		latestHeadByNumberMu.Unlock()
		headers <- h
	}

	lastLongestChainAwaiter.AwaitOrFail(t)
	ht.Stop(t)
	assert.Equal(t, int64(5), ht.headSaver.LatestChain().Number)

	for _, h := range headSeq.Heads {
		c := ht.headSaver.Chain(h.Hash)
		require.NotNil(t, c)
		assert.Equal(t, c.ParentHash, h.ParentHash)
		assert.Equal(t, c.Timestamp.Unix(), h.Timestamp.UTC().Unix())
		assert.Equal(t, c.Number, h.Number)
	}

	checker.AssertExpectations(t)
}

func TestHeadTracker_SwitchesToLongestChainWithHeadSamplingDisabled(t *testing.T) {
	// Need separate db because ht.Stop() will cancel the ctx, causing a db connection
	// close and go-txdb rollback.
	config, db := heavyweight.FullTestDB(t, "switches_longest_chain", true, true)

	config.Overrides.GlobalEvmFinalityDepth = null.IntFrom(50)
	// Need to set the buffer to something large since we inject a lot of heads at once and otherwise they will be dropped
	config.Overrides.GlobalEvmHeadTrackerMaxBufferSize = null.IntFrom(42)
	d := 0 * time.Second
	config.Overrides.GlobalEvmHeadTrackerSamplingInterval = &d

	ethClient, sub := cltest.NewEthClientAndSubMockWithDefaultChain(t)

	checker := new(htmocks.HeadTrackable)
	checker.Test(t)
	orm := headtracker.NewORM(db, logger.TestLogger(t), config, cltest.FixtureChainID)
	evmcfg := evmtest.NewChainScopedConfig(t, config)
	ht := createHeadTrackerWithChecker(t, ethClient, evmcfg, orm, checker)

	chchHeaders := make(chan chan<- *evmtypes.Head, 1)
	ethClient.On("SubscribeNewHead", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { chchHeaders <- args.Get(1).(chan<- *evmtypes.Head) }).
		Return(sub, nil)
	sub.On("Unsubscribe").Return()
	sub.On("Err").Return(nil)

	// ---------------------
	blocks := cltest.NewBlocks(t, 10)

	head0 := blocks.Head(0) // evmtypes.Head{Number: 0, Hash: utils.NewHash(), ParentHash: utils.NewHash(), Timestamp: time.Unix(0, 0)}
	// Initial query
	ethClient.On("HeadByNumber", mock.Anything, (*big.Int)(nil)).Return(head0, nil)

	headSeq := cltest.NewHeadBuffer(t)
	headSeq.Append(blocks.Head(0))
	headSeq.Append(blocks.Head(1))

	// Blocks 2 and 3 are out of order
	headSeq.Append(blocks.Head(3))
	headSeq.Append(blocks.Head(2))

	// Block 4 comes in
	headSeq.Append(blocks.Head(4))

	// Another block at level 4 comes in, that will be uncled
	headSeq.Append(blocks.NewHead(4))

	// Reorg happened forking from block 2
	blocksForked := blocks.ForkAt(t, 2, 5)
	headSeq.Append(blocksForked.Head(2))
	headSeq.Append(blocksForked.Head(3))
	headSeq.Append(blocksForked.Head(4))
	headSeq.Append(blocksForked.Head(5)) // Now the new chain is longer

	lastLongestChainAwaiter := cltest.NewAwaiter()

	checker.On("OnNewLongestChain", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			h := args.Get(1).(*evmtypes.Head)
			require.Equal(t, int64(0), h.Number)
			require.Equal(t, blocks.Head(0).Hash, h.Hash)
		}).Return().Once()

	checker.On("OnNewLongestChain", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			h := args.Get(1).(*evmtypes.Head)
			require.Equal(t, int64(1), h.Number)
			require.Equal(t, blocks.Head(1).Hash, h.Hash)
		}).Return().Once()

	checker.On("OnNewLongestChain", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			h := args.Get(1).(*evmtypes.Head)
			require.Equal(t, int64(3), h.Number)
			require.Equal(t, blocks.Head(3).Hash, h.Hash)
		}).Return().Once()

	checker.On("OnNewLongestChain", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			h := args.Get(1).(*evmtypes.Head)
			require.Equal(t, int64(4), h.Number)
			require.Equal(t, blocks.Head(4).Hash, h.Hash)

			// Check that the block came with its parents
			require.NotNil(t, h.Parent)
			require.Equal(t, h.Parent.Hash, blocks.Head(3).Hash)
			require.NotNil(t, h.Parent.Parent.Hash)
			require.Equal(t, h.Parent.Parent.Hash, blocks.Head(2).Hash)
			require.NotNil(t, h.Parent.Parent.Parent)
			require.Equal(t, h.Parent.Parent.Parent.Hash, blocks.Head(1).Hash)
		}).Return().Once()

	checker.On("OnNewLongestChain", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			h := args.Get(1).(*evmtypes.Head)

			require.Equal(t, int64(5), h.Number)
			require.Equal(t, blocksForked.Head(5).Hash, h.Hash)

			// This is the new longest chain, check that it came with its parents
			require.NotNil(t, h.Parent)
			require.Equal(t, h.Parent.Hash, blocksForked.Head(4).Hash)
			require.NotNil(t, h.Parent.Parent)
			require.Equal(t, h.Parent.Parent.Hash, blocksForked.Head(3).Hash)
			require.NotNil(t, h.Parent.Parent.Parent)
			require.Equal(t, h.Parent.Parent.Parent.Hash, blocksForked.Head(2).Hash)
			require.NotNil(t, h.Parent.Parent.Parent.Parent)
			require.Equal(t, h.Parent.Parent.Parent.Parent.Hash, blocksForked.Head(1).Hash)
			lastLongestChainAwaiter.ItHappened()
		}).Return().Once()

	ht.Start(t)

	headers := <-chchHeaders

	// This grotesque construction is the only way to do dynamic return values using
	// the mock package.  We need dynamic returns because we're simulating reorgs.
	latestHeadByNumber := make(map[int64]*evmtypes.Head)
	latestHeadByNumberMu := new(sync.Mutex)

	fnCall := ethClient.On("HeadByNumber", mock.Anything, mock.Anything)
	fnCall.RunFn = func(args mock.Arguments) {
		latestHeadByNumberMu.Lock()
		defer latestHeadByNumberMu.Unlock()
		num := args.Get(1).(*big.Int)
		head, exists := latestHeadByNumber[num.Int64()]
		if !exists {
			head = cltest.Head(num.Int64())
			latestHeadByNumber[num.Int64()] = head
		}
		fnCall.ReturnArguments = mock.Arguments{head, nil}
	}

	for _, h := range headSeq.Heads {
		latestHeadByNumberMu.Lock()
		latestHeadByNumber[h.Number] = h
		latestHeadByNumberMu.Unlock()
		headers <- h
	}

	lastLongestChainAwaiter.AwaitOrFail(t)
	ht.Stop(t)
	assert.Equal(t, int64(5), ht.headSaver.LatestChain().Number)

	for _, h := range headSeq.Heads {
		c := ht.headSaver.Chain(h.Hash)
		require.NotNil(t, c)
		assert.Equal(t, c.ParentHash, h.ParentHash)
		assert.Equal(t, c.Timestamp.Unix(), h.Timestamp.UTC().Unix())
		assert.Equal(t, c.Number, h.Number)
	}

	checker.AssertExpectations(t)
}

func TestHeadTracker_Backfill(t *testing.T) {
	t.Parallel()

	// Heads are arranged as follows:
	// headN indicates an unpersisted ethereum header
	// hN indicates a persisted head record
	//
	// (1)->(H0)
	//
	//       (14Orphaned)-+
	//                    +->(13)->(12)->(11)->(H10)->(9)->(H8)
	// (15)->(14)---------+

	now := uint64(time.Now().UTC().Unix())

	gethHead0 := &gethTypes.Header{
		Number:     big.NewInt(0),
		ParentHash: gethCommon.BigToHash(big.NewInt(0)),
		Time:       now,
	}
	head0 := evmtypes.NewHead(gethHead0.Number, utils.NewHash(), gethHead0.ParentHash, gethHead0.Time, utils.NewBig(&cltest.FixtureChainID))

	h1 := *cltest.Head(1)
	h1.ParentHash = head0.Hash

	gethHead8 := &gethTypes.Header{
		Number:     big.NewInt(8),
		ParentHash: utils.NewHash(),
		Time:       now,
	}
	head8 := evmtypes.NewHead(gethHead8.Number, utils.NewHash(), gethHead8.ParentHash, gethHead8.Time, utils.NewBig(&cltest.FixtureChainID))

	h9 := *cltest.Head(9)
	h9.ParentHash = head8.Hash

	gethHead10 := &gethTypes.Header{
		Number:     big.NewInt(10),
		ParentHash: h9.Hash,
		Time:       now,
	}
	head10 := evmtypes.NewHead(gethHead10.Number, utils.NewHash(), gethHead10.ParentHash, gethHead10.Time, utils.NewBig(&cltest.FixtureChainID))

	h11 := *cltest.Head(11)
	h11.ParentHash = head10.Hash

	h12 := *cltest.Head(12)
	h12.ParentHash = h11.Hash

	h13 := *cltest.Head(13)
	h13.ParentHash = h12.Hash

	h14Orphaned := *cltest.Head(14)
	h14Orphaned.ParentHash = h13.Hash

	h14 := *cltest.Head(14)
	h14.ParentHash = h13.Hash

	h15 := *cltest.Head(15)
	h15.ParentHash = h14.Hash

	heads := []evmtypes.Head{
		h9,
		h11,
		h12,
		h13,
		h14Orphaned,
		h14,
		h15,
	}

	ctx := context.Background()

	t.Run("does nothing if all the heads are in database", func(t *testing.T) {
		db := pgtest.NewSqlxDB(t)
		cfg := cltest.NewTestGeneralConfig(t)
		logger := logger.TestLogger(t)
		orm := headtracker.NewORM(db, logger, cfg, cltest.FixtureChainID)
		for _, h := range heads {
			require.NoError(t, orm.IdempotentInsertHead(context.TODO(), &h))
		}

		ethClient := cltest.NewEthClientMock(t)
		ethClient.On("ChainID", mock.Anything).Return(cfg.DefaultChainID(), nil)
		ht := createHeadTrackerWithNeverSleeper(t, ethClient, cfg, orm)

		err := ht.Backfill(ctx, &h12, 2)
		require.NoError(t, err)

		ethClient.AssertExpectations(t)
	})

	t.Run("fetches a missing head", func(t *testing.T) {
		db := pgtest.NewSqlxDB(t)
		cfg := cltest.NewTestGeneralConfig(t)
		logger := logger.TestLogger(t)
		orm := headtracker.NewORM(db, logger, cfg, cltest.FixtureChainID)
		for _, h := range heads {
			require.NoError(t, orm.IdempotentInsertHead(context.TODO(), &h))
		}

		ethClient := cltest.NewEthClientMock(t)
		ethClient.On("ChainID", mock.Anything).Return(cfg.DefaultChainID(), nil)
		ethClient.On("HeadByNumber", mock.Anything, big.NewInt(10)).
			Return(&head10, nil)

		ht := createHeadTrackerWithNeverSleeper(t, ethClient, cfg, orm)

		var depth uint = 3

		err := ht.Backfill(ctx, &h12, depth)
		require.NoError(t, err)

		h := ht.headSaver.Chain(h12.Hash)

		assert.Equal(t, int64(12), h.Number)
		require.NotNil(t, h.Parent)
		assert.Equal(t, int64(11), h.Parent.Number)
		require.NotNil(t, h.Parent)
		assert.Equal(t, int64(10), h.Parent.Parent.Number)
		require.NotNil(t, h.Parent.Parent.Parent)
		assert.Equal(t, int64(9), h.Parent.Parent.Parent.Number)

		writtenHead, err := orm.HeadByHash(context.TODO(), head10.Hash)
		require.NoError(t, err)
		assert.Equal(t, int64(10), writtenHead.Number)

		ethClient.AssertExpectations(t)
	})

	t.Run("fetches only heads that are missing", func(t *testing.T) {
		db := pgtest.NewSqlxDB(t)
		cfg := cltest.NewTestGeneralConfig(t)
		logger := logger.TestLogger(t)
		orm := headtracker.NewORM(db, logger, cfg, cltest.FixtureChainID)
		for _, h := range heads {
			require.NoError(t, orm.IdempotentInsertHead(context.TODO(), &h))
		}

		ethClient := cltest.NewEthClientMock(t)
		ethClient.On("ChainID", mock.Anything).Return(cfg.DefaultChainID(), nil)

		ht := createHeadTrackerWithNeverSleeper(t, ethClient, cfg, orm)

		ethClient.On("HeadByNumber", mock.Anything, big.NewInt(10)).
			Return(&head10, nil)
		ethClient.On("HeadByNumber", mock.Anything, big.NewInt(8)).
			Return(&head8, nil)

		// Needs to be 8 because there are 8 heads in chain (15,14,13,12,11,10,9,8)
		var depth uint = 8

		err := ht.Backfill(ctx, &h15, depth)
		require.NoError(t, err)

		h := ht.headSaver.Chain(h15.Hash)

		require.Equal(t, uint32(8), h.ChainLength())
		earliestInChain := h.EarliestInChain()
		assert.Equal(t, head8.Number, earliestInChain.Number)
		assert.Equal(t, head8.Hash, earliestInChain.Hash)

		ethClient.AssertExpectations(t)
	})

	t.Run("does not backfill if chain length is already greater than or equal to depth", func(t *testing.T) {
		db := pgtest.NewSqlxDB(t)
		cfg := cltest.NewTestGeneralConfig(t)
		logger := logger.TestLogger(t)
		orm := headtracker.NewORM(db, logger, cfg, cltest.FixtureChainID)
		for _, h := range heads {
			require.NoError(t, orm.IdempotentInsertHead(context.TODO(), &h))
		}

		ethClient := cltest.NewEthClientMock(t)
		ethClient.On("ChainID", mock.Anything).Return(cfg.DefaultChainID(), nil)

		ht := createHeadTrackerWithNeverSleeper(t, ethClient, cfg, orm)

		err := ht.Backfill(ctx, &h15, 3)
		require.NoError(t, err)

		err = ht.Backfill(ctx, &h15, 5)
		require.NoError(t, err)

		ethClient.AssertExpectations(t)
	})

	t.Run("only backfills to height 0 if chain length would otherwise cause it to try and fetch a negative head", func(t *testing.T) {
		db := pgtest.NewSqlxDB(t)
		cfg := cltest.NewTestGeneralConfig(t)
		logger := logger.TestLogger(t)
		orm := headtracker.NewORM(db, logger, cfg, cltest.FixtureChainID)

		ethClient := cltest.NewEthClientMock(t)
		ethClient.On("ChainID", mock.Anything).Return(cfg.DefaultChainID(), nil)
		ethClient.On("HeadByNumber", mock.Anything, big.NewInt(0)).
			Return(&head0, nil)

		require.NoError(t, orm.IdempotentInsertHead(context.TODO(), &h1))

		ht := createHeadTrackerWithNeverSleeper(t, ethClient, cfg, orm)

		err := ht.Backfill(ctx, &h1, 400)
		require.NoError(t, err)

		h := ht.headSaver.Chain(h1.Hash)
		require.NotNil(t, h)

		require.Equal(t, uint32(2), h.ChainLength())
		require.Equal(t, int64(0), h.EarliestInChain().Number)

		ethClient.AssertExpectations(t)
	})

	t.Run("abandons backfill and returns error if the eth node returns not found", func(t *testing.T) {
		db := pgtest.NewSqlxDB(t)
		cfg := cltest.NewTestGeneralConfig(t)
		logger := logger.TestLogger(t)
		orm := headtracker.NewORM(db, logger, cfg, cltest.FixtureChainID)
		for _, h := range heads {
			require.NoError(t, orm.IdempotentInsertHead(context.TODO(), &h))
		}

		ethClient := cltest.NewEthClientMock(t)
		ethClient.On("ChainID", mock.Anything).Return(cfg.DefaultChainID(), nil)
		ethClient.On("HeadByNumber", mock.Anything, big.NewInt(10)).
			Return(&head10, nil).
			Once()
		ethClient.On("HeadByNumber", mock.Anything, big.NewInt(8)).
			Return(nil, ethereum.NotFound).
			Once()

		ht := createHeadTrackerWithNeverSleeper(t, ethClient, cfg, orm)

		err := ht.Backfill(ctx, &h12, 400)
		require.Error(t, err)
		require.EqualError(t, err, "fetchAndSaveHead failed: not found")

		h := ht.headSaver.Chain(h12.Hash)

		// Should contain 12, 11, 10, 9
		assert.Equal(t, 4, int(h.ChainLength()))
		assert.Equal(t, int64(9), h.EarliestInChain().Number)

		ethClient.AssertExpectations(t)
	})

	t.Run("abandons backfill and returns error if the context time budget is exceeded", func(t *testing.T) {
		db := pgtest.NewSqlxDB(t)
		cfg := cltest.NewTestGeneralConfig(t)
		logger := logger.TestLogger(t)
		orm := headtracker.NewORM(db, logger, cfg, cltest.FixtureChainID)
		for _, h := range heads {
			require.NoError(t, orm.IdempotentInsertHead(context.TODO(), &h))
		}

		ethClient := cltest.NewEthClientMock(t)
		ethClient.On("ChainID", mock.Anything).Return(cfg.DefaultChainID(), nil)
		ethClient.On("HeadByNumber", mock.Anything, big.NewInt(10)).
			Return(&head10, nil)
		ethClient.On("HeadByNumber", mock.Anything, big.NewInt(8)).
			Return(nil, context.DeadlineExceeded)

		ht := createHeadTrackerWithNeverSleeper(t, ethClient, cfg, orm)

		err := ht.Backfill(ctx, &h12, 400)
		require.Error(t, err)
		require.EqualError(t, err, "fetchAndSaveHead failed: context deadline exceeded")

		h := ht.headSaver.Chain(h12.Hash)

		// Should contain 12, 11, 10, 9
		assert.Equal(t, 4, int(h.ChainLength()))
		assert.Equal(t, int64(9), h.EarliestInChain().Number)

		ethClient.AssertExpectations(t)
	})
}

func createHeadTracker(t *testing.T, ethClient evmclient.Client, config headtracker.Config, orm headtracker.ORM) *headTrackerUniverse {
	lggr := logger.TestLogger(t)
	hb := headtracker.NewHeadBroadcaster(lggr)
	hs := headtracker.NewHeadSaver(lggr, orm, config)
	return &headTrackerUniverse{
		mu:              new(sync.Mutex),
		headTracker:     headtracker.NewHeadTracker(lggr, ethClient, config, hb, hs),
		headBroadcaster: hb,
		headSaver:       hs,
	}
}

func createHeadTrackerWithNeverSleeper(t testing.TB, ethClient evmclient.Client, cfg *configtest.TestGeneralConfig, orm headtracker.ORM) *headTrackerUniverse {
	evmcfg := evmtest.NewChainScopedConfig(t, cfg)
	lggr := logger.TestLogger(t)
	hb := headtracker.NewHeadBroadcaster(lggr)
	hs := headtracker.NewHeadSaver(lggr, orm, evmcfg)
	ht := headtracker.NewHeadTracker(lggr, ethClient, evmcfg, hb, hs)
	_, err := hs.LoadFromDB(context.Background())
	require.NoError(t, err)
	return &headTrackerUniverse{
		mu:              new(sync.Mutex),
		headTracker:     ht,
		headBroadcaster: hb,
		headSaver:       hs,
	}
}

func createHeadTrackerWithChecker(t *testing.T, ethClient evmclient.Client, config headtracker.Config, orm headtracker.ORM, checker httypes.HeadTrackable) *headTrackerUniverse {
	lggr := logger.TestLogger(t)
	hb := headtracker.NewHeadBroadcaster(lggr)
	hs := headtracker.NewHeadSaver(lggr, orm, config)
	hb.Subscribe(checker)
	ht := headtracker.NewHeadTracker(lggr, ethClient, config, hb, hs)
	return &headTrackerUniverse{
		mu:              new(sync.Mutex),
		headTracker:     ht,
		headBroadcaster: hb,
		headSaver:       hs,
	}
}

type headTrackerUniverse struct {
	mu              *sync.Mutex
	stopped         bool
	headTracker     httypes.HeadTracker
	headBroadcaster httypes.HeadBroadcaster
	headSaver       httypes.HeadSaver
}

func (u *headTrackerUniverse) Backfill(ctx context.Context, head *evmtypes.Head, depth uint) error {
	return u.headTracker.Backfill(ctx, head, depth)
}

func (u *headTrackerUniverse) Start(t *testing.T) {
	u.mu.Lock()
	defer u.mu.Unlock()
	require.NoError(t, u.headBroadcaster.Start())
	require.NoError(t, u.headTracker.Start())
	t.Cleanup(func() {
		u.Stop(t)
	})
}

func (u *headTrackerUniverse) Stop(t *testing.T) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.stopped {
		return
	}
	u.stopped = true
	require.NoError(t, u.headBroadcaster.Close())
	require.NoError(t, u.headTracker.Close())
}
