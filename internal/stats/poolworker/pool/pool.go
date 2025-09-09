package pool

import (
	"dex-stats-sol/internal/consts"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/eventadapter"
	"dex-stats-sol/internal/stats/poolworker/shared"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"github.com/cespare/xxhash/v2"
	"math"
	"sync"
	"sync/atomic"
)

// ---------------------- Pool 类型 ----------------------

// Pool 表示一个交易池的状态，包括基础信息、动态状态、时间窗口统计和链上状态
type Pool struct {
	// --- 基础信息 ---
	Address     types.Pubkey  // 池子地址，唯一标识
	AddressHash uint64        // 地址哈希值，用于稳定排序
	Dex         uint16        // 所属 DEX 类型
	WorkerID    uint8         // 工作线程/分区编号
	createdAt   atomic.Uint32 // 池子创建时间（秒）

	// --- Token 信息 ---
	BaseToken     types.Pubkey // base token 地址
	QuoteToken    types.Pubkey // quote token 地址
	IsNativeQuote bool         // 快速判断 quote 是否是链的原生代币（如 SOL/WSOL、BNB/WBNB）
	IsUSDCQuote   bool         // 快速判断 quote 是否是 USDC
	BaseDecimals  uint8        // base token 精度
	QuoteDecimals uint8        // quote token 精度

	// --- 池子代币账户 ---
	BaseTokenAccount  types.Pubkey // base token 的池子账户地址
	QuoteTokenAccount types.Pubkey // quote token 的池子账户地址

	// --- 杠杆信息 ---
	longLeverage  atomic.Int32  // 多头杠杆
	shortLeverage atomic.Int32  // 空头杠杆
	listingAtMs   atomic.Uint64 // 上线时间

	// --- 排序/关键指标（原子字段，方便并发读取） ---
	priceUsd         utils.AtomicFloat64 // 最新价格（USD）
	liq              utils.AtomicFloat64 // 流动性总额（USD）
	fdv              utils.AtomicFloat64 // Fully Diluted Value
	marketCap        utils.AtomicFloat64 // 市值
	top10HolderRatio utils.AtomicFloat64 // Top10 占比
	holderCount      atomic.Uint32       // 持有人数

	// --- 原始精确数据 ---
	liquidity         *LiquidityInfo      // 流动性详细数据
	SharedSupply      *shared.SupplyInfo  // 所有 pool 共享的 token 级别供应信息
	SharedHolderCount *shared.HolderCount // 所有 pool 共享的 token 级别 HolderCount
	SharedTopHolders  *shared.TopHolders  // 所有 pool 共享的 token 级别 TopHolders

	// --- 时间窗口级别统计（用于排序和聚合计算） ---
	Windows [types.WindowCount]WindowData // 时间窗口统计数据数组（1m、5m、1h、6h、24h等）

	// --- 链上事件状态 ---
	lastChainEventTs    atomic.Uint32 // 最近处理链上事件时间戳
	lastSwapEventTs     atomic.Uint32 // 最近交易事件时间戳
	lastPushBlockNumber uint32        // 最近推送到 Kafka 的 blockNumber
	blockNumber         uint32        // 当前处理 BlockNumber 编号
	blockBitmap         Bitmap        // 最近若干 blockNumber 状态位图

	// --- 成交聚合数据（不同粒度） ---
	tickerMu  sync.Mutex
	tickers1s *Tickers // 1s 粒度，计算 1m、5m 窗口
	tickers1m *Tickers // 1m 粒度，计算 1h 窗口
	tickers2m *Tickers // 2m 粒度，计算 6h、24h 窗口

	// --- 查询缓存（避免重复计算） ---
	poolAddress    string                        // 池子地址 string 格式，加快查询速度
	tokenAddress   string                        // base token 地址 string 格式
	poolStatsCache atomic.Pointer[pb.TickerData] // 最近一次查询结果缓存
}

// ---------------------- Pool 构造函数 ----------------------

// PoolInitParams 用于 NewPool 构造函数
type PoolInitParams struct {
	Data          *eventadapter.PoolEvents
	WorkerID      uint8
	LongLeverage  int32
	ShortLeverage int32
	ListingAtMs   uint64
	LatestTime    uint32
	QuotePriceUSD float64
	SupplyInfo    *shared.SupplyInfo
	HolderCount   *shared.HolderCount
	TopHolders    *shared.TopHolders
}

func NewPool(params *PoolInitParams) *Pool {
	data := params.Data
	agg := data.Aggregate()
	if agg.OpenPrice == 0 {
		logger.Infof("[Pool] openPrice is zero, pool=%s", data.Pool)
		return nil
	}

	// 初始化 Pool 基础信息
	p := &Pool{
		Address:     data.Pool,
		AddressHash: xxhash.Sum64(data.Pool[:]),
		Dex:         data.Dex,
		WorkerID:    params.WorkerID,

		BaseToken:     data.BaseToken,
		QuoteToken:    data.QuoteToken,
		IsNativeQuote: data.QuoteToken == consts.WNativeToken || data.QuoteToken == consts.NativeToken,
		IsUSDCQuote:   data.QuoteToken == consts.USDCMint,
		BaseDecimals:  data.BaseDecimals,
		QuoteDecimals: data.QuoteDecimals,

		BaseTokenAccount:  data.BaseTokenAccount,
		QuoteTokenAccount: data.QuoteTokenAccount,

		liquidity:         NewLiquidityInfo(),
		SharedSupply:      params.SupplyInfo,
		SharedHolderCount: params.HolderCount,
		SharedTopHolders:  params.TopHolders,

		blockNumber: data.BlockNumber,
		tickers1s:   NewTickers1s(agg.OpenPrice, data.BlockTime),
		tickers1m:   NewTickers1m(agg.OpenPrice, data.BlockTime),
		tickers2m:   NewTickers2m(agg.OpenPrice, data.BlockTime),

		poolAddress:  data.Pool.String(),
		tokenAddress: data.BaseToken.String(),
	}

	// 设置杠杆信息
	p.SetLongLeverage(params.LongLeverage)
	p.SetShortLeverage(params.ShortLeverage)
	if p.HasLeverage() {
		p.listingAtMs.Store(params.ListingAtMs)
		logger.Debugf("[Pool] leverage detected: long=%d, short=%d, listingAt=%d, addr=%s",
			params.LongLeverage, params.ShortLeverage, params.ListingAtMs, params.Data.Pool)
	}

	// 设置创建时间和事件时间戳
	if data.IsNewCreate {
		p.setCreatedAt(data.BlockTime)
	}
	p.setLastChainEventTs(data.BlockTime)
	p.setLastSwapEventTs(data.BlockTime)
	p.markBlockProcessed(data.BlockNumber)

	// 初始化 windows
	for i := range p.Windows {
		p.Windows[i].setWindowSize(WindowSizes[i])
		p.Windows[i].setOpenPrice(agg.OpenPrice)
		p.Windows[i].setOpenPriceTs(data.BlockTime)
	}

	// 插入初始 ticker
	ticker := TickerFromAggregate(agg)
	insertPos1s := p.tickers1s.addTicker(ticker, params.LatestTime) // 1秒粒度
	insertPos1m := p.tickers1m.addTicker(ticker, params.LatestTime) // 1分钟粒度
	insertPos2m := p.tickers2m.addTicker(ticker, params.LatestTime) // 2分钟粒度
	p.warnIfPriceMismatch()
	p.updateWindows(ticker, insertPos1s, insertPos1m, insertPos2m, params.LatestTime)

	// 设置价格和流动性信息
	p.setPriceUsd(p.tickers2m.currentPrice)
	p.liquidity.SetLiquidityInfo(agg.BaseLiq, agg.QuoteLiq, p.PriceUsd())

	// 更新池子核心指标
	p.UpdateLiquidity(params.QuotePriceUSD)
	p.UpdateMarketCapFDV()
	p.UpdateHolderCount()
	p.UpdateTop10HolderRatio()

	return p
}

// ---------------------- 数据更新方法 ----------------------

func (p *Pool) Update(data *eventadapter.PoolEvents, latestTime uint32, quotePriceUsd float64) {
	if p.isBlockProcessed(data.BlockNumber) {
		return
	}

	// 先聚合计算，放锁外，减少锁内耗时
	agg := data.Aggregate()
	ticker := TickerFromAggregate(agg)

	p.tickerMu.Lock()
	defer p.tickerMu.Unlock()

	// 设置时间戳
	p.setLastChainEventTs(data.BlockTime)
	if ticker.IsValid() {
		// 设置 Swap 时间戳
		p.setLastSwapEventTs(data.BlockTime)

		// 插入 ticker, 并更新 windows
		insertPos1s := p.tickers1s.addTicker(ticker, latestTime)
		insertPos1m := p.tickers1m.addTicker(ticker, latestTime)
		insertPos2m := p.tickers2m.addTicker(ticker, latestTime)
		p.warnIfPriceMismatch()
		p.updateWindows(ticker, insertPos1s, insertPos1m, insertPos2m, latestTime)

		// 设置价格
		p.setPriceUsd(p.tickers2m.currentPrice)
	}

	// 设置流动性信息
	if data.BlockNumber > p.blockNumber {
		p.liquidity.SetLiquidityInfo(agg.BaseLiq, agg.QuoteLiq, p.PriceUsd())
	}

	// 更新流动性和市值
	p.UpdateLiquidity(quotePriceUsd)
	p.UpdateMarketCapFDV()
	p.markBlockProcessed(data.BlockNumber)
}

// SlideOutExpired 滑出过期 ticker 并更新窗口
func (p *Pool) SlideOutExpired(latestTime uint32) (updatedWindows [types.WindowCount]bool) {
	p.tickerMu.Lock()
	defer p.tickerMu.Unlock()

	// 1s ticker - 短窗口（1m / 5m）
	priceChanged1s := p.tickers1s.removeExpired(latestTime)
	updatedWindows[types.Window1Min] = p.tickers1s.slideOutWindow(&p.Windows[types.Window1Min], latestTime, priceChanged1s)
	updatedWindows[types.Window5Min] = p.tickers1s.slideOutWindow(&p.Windows[types.Window5Min], latestTime, priceChanged1s)

	// 1m ticker - 中窗口（1h）
	priceChanged1m := p.tickers1m.removeExpired(latestTime)
	updatedWindows[types.Window1H] = p.tickers1m.slideOutWindow(&p.Windows[types.Window1H], latestTime, priceChanged1m)

	// 2m ticker - 长窗口（6h / 24h）
	priceChanged2m := p.tickers2m.removeExpired(latestTime)
	updatedWindows[types.Window6H] = p.tickers2m.slideOutWindow(&p.Windows[types.Window6H], latestTime, priceChanged2m)
	updatedWindows[types.Window24H] = p.tickers2m.slideOutWindow(&p.Windows[types.Window24H], latestTime, priceChanged2m)

	return updatedWindows
}

// ---------------------- 原子更新方法 ----------------------

// RestoreSharedTokenData 替换 Pool 的共享 token 级别数据。
// 注意: 用于快照恢复时从 TokenMap 重建 Pool 的共享状态，不应用于常规业务更新。
func (p *Pool) RestoreSharedTokenData(
	supply *shared.SupplyInfo,
	holderCount *shared.HolderCount,
	topHolders *shared.TopHolders,
) {
	p.SharedSupply = supply
	p.SharedHolderCount = holderCount
	p.SharedTopHolders = topHolders
}

func (p *Pool) UpdateLiquidity(quotePriceUsd float64) {
	liquidity := p.liquidity.QuoteLiq() * quotePriceUsd * 2
	p.liq.Store(utils.Float64Round(liquidity))
}

func (p *Pool) UpdateHolderCount() {
	p.holderCount.Store(p.SharedHolderCount.Value())
}

func (p *Pool) UpdateTop10HolderRatio() {
	ratio := float64(0)
	totalSupply := p.TotalSupply()
	if totalSupply > 0 {
		top10Balance := p.SharedTopHolders.Top10Balance()
		ratio = top10Balance / totalSupply
	}
	p.top10HolderRatio.Store(ratio)
}

func (p *Pool) UpdateMarketCapFDV() {
	// 获取当前价格
	priceUsd := p.PriceUsd()
	if priceUsd <= 0 {
		// 价格为0或负值时，市值和 FDV 都置零
		p.setMarketCap(0)
		p.setFDV(0)
		return
	}

	_, maxSupply, circulatingSupply := p.supplyInfo()
	p.setMarketCap(utils.Float64Round(circulatingSupply * priceUsd))
	p.setFDV(utils.Float64Round(maxSupply * priceUsd))
}

func (p *Pool) SupplyStr() (string, string, string) {
	totalSupply, maxSupply, circulatingSupply := p.supplyInfo()
	totalSupplyStr := utils.FastFloatToStr(totalSupply)

	maxSupplyStr := utils.FastFloatToStr(maxSupply)
	if maxSupply == totalSupply {
		maxSupplyStr = totalSupplyStr
	}

	circulatingSupplyStr := utils.FastFloatToStr(circulatingSupply)
	if circulatingSupply == totalSupply {
		circulatingSupplyStr = totalSupplyStr
	}

	return totalSupplyStr, maxSupplyStr, circulatingSupplyStr
}

func (p *Pool) SetLongLeverage(value int32) bool {
	old := p.longLeverage.Swap(value)
	return old != value
}

func (p *Pool) SetShortLeverage(value int32) bool {
	old := p.shortLeverage.Swap(value)
	return old != value
}

func (p *Pool) UpdateListingAt(newListingAtMs uint64) bool {
	if p.HasLeverage() {
		// 有杠杆时，listingAt 必须 > 0
		if newListingAtMs == 0 {
			return false
		}
		if old := p.listingAtMs.Load(); old != newListingAtMs {
			p.listingAtMs.Store(newListingAtMs)
			return true
		}
	} else {
		// 无杠杆时，listingAt 必须清零
		if p.listingAtMs.Load() != 0 {
			p.listingAtMs.Store(0)
			return true
		}
	}
	return false
}

// SetLastPushBlockNumber 更新最近一次已推送的区块高度。
func (p *Pool) SetLastPushBlockNumber(blockNumber uint32) {
	if blockNumber > p.lastPushBlockNumber {
		p.lastPushBlockNumber = blockNumber
	}
}

// ShouldPushTicker 判断当前 Pool 是否需要推送最新的 Ticker 数据。
func (p *Pool) ShouldPushTicker() bool {
	return p.lastPushBlockNumber < p.blockNumber
}

func (p *Pool) LastChainEventTs() uint32 {
	return p.lastChainEventTs.Load()
}

func (p *Pool) LastSwapEventTs() uint32 {
	return p.lastSwapEventTs.Load()
}

func (p *Pool) CreatedAt() uint32 {
	return p.createdAt.Load()
}

func (p *Pool) PriceUsd() float64 {
	return p.priceUsd.Load()
}

func (p *Pool) LongLeverage() int32 {
	return p.longLeverage.Load()
}

func (p *Pool) ShortLeverage() int32 {
	return p.shortLeverage.Load()
}

func (p *Pool) ListingAtMs() uint64 {
	return p.listingAtMs.Load()
}

func (p *Pool) HasLeverage() bool {
	return p.longLeverage.Load() != 0 || p.shortLeverage.Load() != 0
}

func (p *Pool) NoLeverage() bool {
	return !p.HasLeverage()
}

func (p *Pool) Liquidity() float64 {
	return p.liq.Load()
}

func (p *Pool) MarketCap() float64 {
	return p.marketCap.Load()
}

func (p *Pool) FDV() float64 {
	return p.fdv.Load()
}

func (p *Pool) HolderCount() uint32 {
	return p.holderCount.Load()
}

func (p *Pool) Top10HolderRatio() float64 {
	return p.top10HolderRatio.Load()
}

func (p *Pool) BlockNumber() uint32 {
	return p.blockNumber
}

func (p *Pool) TotalSupply() float64 {
	totalSupply := p.SharedSupply.TotalSupply()
	totalSupply = max(totalSupply, math.Floor(p.SharedTopHolders.Top10Balance()))
	return totalSupply
}

func (p *Pool) supplyInfo() (float64, float64, float64) {
	totalSupply := p.TotalSupply()
	maxSupply := max(p.SharedSupply.MaxSupply(), totalSupply)

	circulatingSupply := p.SharedSupply.CirculatingSupply()
	if circulatingSupply == 0 || circulatingSupply > totalSupply {
		circulatingSupply = totalSupply
	}
	return totalSupply, maxSupply, circulatingSupply
}

func (p *Pool) setLastChainEventTs(ts uint32) {
	old := p.lastChainEventTs.Load()
	if ts > old {
		p.lastChainEventTs.Store(ts)
	}
}

func (p *Pool) setLastSwapEventTs(ts uint32) {
	old := p.lastSwapEventTs.Load()
	if ts > old {
		p.lastSwapEventTs.Store(ts)
	}
}

func (p *Pool) setCreatedAt(createdAt uint32) {
	p.createdAt.Store(createdAt)
}

func (p *Pool) setPriceUsd(price float64) {
	p.priceUsd.Store(price)
}

func (p *Pool) setMarketCap(marketCap float64) {
	p.marketCap.Store(marketCap)
}

func (p *Pool) setFDV(fdv float64) {
	p.fdv.Store(fdv)
}

// isBlockProcessed 判断指定 blockNumber 是否已处理。
func (p *Pool) isBlockProcessed(blockNumber uint32) bool {
	if blockNumber > p.blockNumber {
		return false
	}
	return p.blockBitmap.isSet(p.blockNumber - blockNumber)
}

// markBlockProcessed 标记 blockNumber 已处理。
// 若 blockNumber 超过当前 p.blockNumber，需要右移位图腾出空间，并更新 p.blockNumber。
func (p *Pool) markBlockProcessed(blockNumber uint32) {
	if blockNumber > p.blockNumber {
		p.blockBitmap.shiftRight(blockNumber - p.blockNumber)
		p.blockNumber = blockNumber
	}
	p.blockBitmap.setBit(p.blockNumber - blockNumber)
}

func (p *Pool) warnIfPriceMismatch() {
	if p.tickers1s.currentPrice != p.tickers1m.currentPrice || p.tickers1s.currentPrice != p.tickers2m.currentPrice {
		logger.Warnf("currentPrice mismatch: 1s=%.8f, 1m=%.8f, 2m=%.8f, pool=%s",
			p.tickers1s.currentPrice,
			p.tickers1m.currentPrice,
			p.tickers2m.currentPrice,
			p.Address)
	}
}

// updateWindows 根据不同粒度 tickers 的插入位置，更新各时间窗口的聚合统计
func (p *Pool) updateWindows(ticker Ticker, pos1s, pos1m, pos2m AddTickerResult, latestTime uint32) {
	p.tickers1s.addTickerToWindow(&p.Windows[types.Window1Min], ticker, pos1s, latestTime)
	p.tickers1s.addTickerToWindow(&p.Windows[types.Window5Min], ticker, pos1s, latestTime)
	p.tickers1m.addTickerToWindow(&p.Windows[types.Window1H], ticker, pos1m, latestTime)
	p.tickers2m.addTickerToWindow(&p.Windows[types.Window6H], ticker, pos2m, latestTime)
	p.tickers2m.addTickerToWindow(&p.Windows[types.Window24H], ticker, pos2m, latestTime)
}

func (p *Pool) Serialize(buf []byte) ([]byte, error) {
	p.tickerMu.Lock()
	defer p.tickerMu.Unlock()

	return SerializePool(p, buf)
}
