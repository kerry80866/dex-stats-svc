package stats

import (
	"context"
	"dex-stats-sol/internal/consts"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"runtime/debug"
)

const (
	ErrCodeBase         = 70000
	ErrCodePanic        = ErrCodeBase + 32
	ErrCodeInvalidParam = ErrCodeBase + 1
	ErrCodeUnavailable  = ErrCodeBase + 2
)

const MAX_PAGE_SIZE = 1000

type FilterFunc func(types.RankingItem[*pool.Pool]) bool

func (app *App) GetRankingList(ctx context.Context, req *pb.GetRankingListRequest) (resp *pb.GetRankingListResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("panic in GetRankingList: %v\nstacktrace:\n%s", r, debug.Stack())
			err = status.Errorf(codes.Internal, "[%d] server panic", ErrCodePanic)
		}
	}()

	// 1. 检查服务是否就绪
	if !app.rankingWorker.IsReady() {
		return nil, status.Errorf(codes.Internal, "[%d] server not registered with Nacos", ErrCodeUnavailable)
	}

	// 2. 检查排序顺序
	if req.SortOrder != "asc" && req.SortOrder != "desc" {
		return nil, status.Errorf(codes.Internal, "[%d] invalid sort_order: %s", ErrCodeInvalidParam, req.SortOrder)
	}

	// 3. 分页参数
	pageSize := int(req.PageSize)
	if pageSize == 0 {
		pageSize = 100
	}
	if pageSize < 1 || pageSize > MAX_PAGE_SIZE {
		return nil, status.Errorf(codes.InvalidArgument, "[%d] exceeds max allowed size of %d, got %d", ErrCodeInvalidParam, MAX_PAGE_SIZE, pageSize)
	}

	// 4. 解析 RankingKey
	rankingKey, filterFunc, err := parseRankingKey(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "[%d] invalid ranking request: %v", ErrCodeInvalidParam, err)
	}

	// 5. 获取数据
	all := app.rankingWorker.GetRanking(rankingKey)
	totalCount := len(all)
	if totalCount == 0 {
		logger.Warnf("no ranking data found for key: %+v", rankingKey)
		return &pb.GetRankingListResponse{Ranking: []*pb.TickerData{}}, nil
	}

	// 6. 构建分页结果
	size := min(pageSize, totalCount)
	list := make([]*pb.TickerData, 0, size)

	// 分开处理, 性能优先
	if filterFunc != nil {
		listLen := len(list)
		if req.SortOrder == "desc" {
			for _, item := range all {
				if filterFunc(item) {
					list = append(list, item.TickerData)
					listLen++
					if listLen >= size {
						break
					}
				}
			}
		} else {
			for i := totalCount - 1; i >= 0; i-- {
				if filterFunc(all[i]) {
					list = append(list, all[i].TickerData)
					listLen++
					if listLen >= size {
						break
					}
				}
			}
		}
	} else {
		if req.SortOrder == "desc" {
			for i := 0; i < size; i++ {
				list = append(list, all[i].TickerData)
			}
		} else {
			start := totalCount - size
			for i := totalCount - 1; i >= start; i-- {
				list = append(list, all[i].TickerData)
			}
		}
	}

	// 7. 返回结果
	return &pb.GetRankingListResponse{
		Ranking: list,
	}, nil
}

func (app *App) GetTickers(ctx context.Context, req *pb.GetTickersRequest) (resp *pb.GetTickersResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("panic in GetTickers: %v\nstacktrace:\n%s", r, debug.Stack())
			err = status.Errorf(codes.Internal, "[%d] server panic", ErrCodePanic)
		}
	}()

	// 1. 检查服务是否就绪
	if !app.rankingWorker.IsReady() {
		return nil, status.Errorf(codes.Internal, "[%d] server not registered with Nacos", ErrCodeUnavailable)
	}

	// 2. 检查请求池数量是否超出限制
	n := len(req.PairAddresses)
	if n > MAX_PAGE_SIZE {
		return nil, status.Errorf(codes.InvalidArgument, "[%d] exceeds max allowed size of %d, got %d", ErrCodeInvalidParam, MAX_PAGE_SIZE, n)
	}

	// 3. 如果请求为空，直接返回空列表
	if n == 0 {
		return &pb.GetTickersResponse{Tickers: []*pb.TickerData{}}, nil
	}

	// 4. 构建索引映射和路由映射
	indexMap := make(map[types.Pubkey][]int, n)
	routes := make(map[uint8][]types.Pubkey, app.poolWorkerCount)
	for i, addr := range req.PairAddresses {
		key, pubkeyErr := types.TryPubkeyFromString(addr)
		if pubkeyErr != nil {
			continue
		}

		// 计算 workerID
		workerID := uint8(utils.PartitionHash(key[:]) % uint32(app.poolWorkerCount))

		// 预分配容量，减少扩容
		if _, exists := routes[workerID]; !exists {
			capacity := max(1, min(n, n*2/int(app.poolWorkerCount)))
			routes[workerID] = make([]types.Pubkey, 0, capacity)
		}

		// 将池地址索引存储到 indexMap 中
		indexMap[key] = append(indexMap[key], i)
		routes[workerID] = append(routes[workerID], key)
	}

	// 5. 各 worker 拉取对应池信息
	tickers := make([]*pb.TickerData, n)
	for workerID, addrs := range routes {
		worker := app.poolWorkers[workerID]
		pools, hotPools := worker.GetPools(addrs)

		// 处理 pools
		for _, pool := range pools {
			ticker := pool.GetPoolStatsData()
			for _, idx := range indexMap[pool.Address] {
				if tickers[idx] == nil {
					tickers[idx] = ticker
				}
			}
		}

		// 处理 hotPools
		for poolAddr, hotData := range hotPools {
			if hotData.LongLeverage == 0 && hotData.ShortLeverage == 0 {
				continue
			}

			// 只构造一次 TickerData
			ticker := &pb.TickerData{
				Chain:        consts.ChainName,
				PairAddress:  poolAddr.String(),
				TokenAddress: "",
				TradeParams: &pb.MMReport{
					LongLeverage:  hotData.LongLeverage,
					ShortLeverage: hotData.ShortLeverage,
					ListingTime:   int64(hotData.ListingAtMs / 1000), // 转换成秒
				},
			}

			// 将 TickerData 填充到所有相应的索引位置
			for _, idx := range indexMap[poolAddr] {
				if tickers[idx] == nil {
					tickers[idx] = ticker
				}
			}
		}
	}

	// 6. 对未找到的池补充默认数据
	for i, addr := range req.PairAddresses {
		if tickers[i] == nil {
			tickers[i] = &pb.TickerData{
				Chain:        consts.ChainName,
				PairAddress:  addr,
				TokenAddress: "",
			}
		}
	}

	return &pb.GetTickersResponse{Tickers: tickers}, nil
}

func (app *App) GetStatsByToken(ctx context.Context, req *pb.GetStatsByTokenRequest) (resp *pb.GetStatsByTokenResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("panic in GetStatsByToken: %v\nstacktrace:\n%s", r, debug.Stack())
			err = status.Errorf(codes.Internal, "[%d] server panic", ErrCodePanic)
		}
	}()

	// 1. 校验 token 地址是否合法
	tokenAddr, pubkeyErr := types.TryPubkeyFromString(req.TokenAddress)
	if pubkeyErr != nil {
		return nil, status.Errorf(codes.Internal, "[%d] invalid token address: %v", ErrCodeInvalidParam, req.TokenAddress)
	}

	// 2. 检查服务是否就绪
	if !app.rankingWorker.IsReady() {
		return nil, status.Errorf(codes.Internal, "[%d] server not registered with Nacos", ErrCodeUnavailable)
	}

	// 3. 遍历各 poolWorker，尝试获取 token 信息
	var top10Balance float64
	var totalSupply float64
	var holderCount uint32
	for _, w := range app.poolWorkers {
		tokenInfo := w.GetTokenInfo(tokenAddr)
		if tokenInfo == nil {
			continue
		}

		if ts := tokenInfo.SharedSupply.TotalSupply(); ts != 0 {
			totalSupply = ts
		}
		if th := tokenInfo.SharedTopHolders.Top10Balance(); th != 0 {
			top10Balance = th
		}
		if hc := tokenInfo.SharedHolderCount.Value(); hc != 0 {
			holderCount = hc
		}

		if totalSupply != 0 && top10Balance != 0 && holderCount != 0 {
			break
		}
	}

	// 4. 计算 Top10 持仓占比
	var top10HoldersRate float64
	if totalSupply != 0 {
		top10HoldersRate = min(top10Balance/totalSupply, 1.0)
	}

	// 5. 构建返回结果
	var h *pb.HolderDistribution
	if top10HoldersRate != 0 || holderCount != 0 {
		h = &pb.HolderDistribution{
			Top_10HoldersRate: top10HoldersRate,
			HolderCount:       int64(holderCount),
		}
	}
	return &pb.GetStatsByTokenResponse{
		Stats: &pb.TokenStats{
			TokenAddress:       req.TokenAddress,
			HolderDistribution: h,
		},
	}, nil
}

func (app *App) GetQuotePrices(ctx context.Context, req *pb.GetQuotePriceRequest) (resp *pb.GetQuotePriceResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("panic in GetQuotePrices: %v\nstacktrace:\n%s", r, debug.Stack())
			err = status.Errorf(codes.Internal, "[%d] server panic", ErrCodePanic)
		}
	}()

	// 1. 检查服务是否就绪
	if !app.rankingWorker.IsReady() {
		return nil, status.Errorf(codes.Internal, "[%d] server not registered with Nacos", ErrCodeUnavailable)
	}

	// 2. 初始化返回数据切片
	prices := make([]*pb.QuotePrice, 0, len(req.QuoteAddresses))
	qp := app.globalQuotePrices.Load()

	// 3. 遍历请求中的每个 quote 地址
	for _, addr := range req.QuoteAddresses {
		priceItem := &pb.QuotePrice{
			QuoteAddress: addr,
			PriceUsd:     0,
			Decimals:     0,
			Valid:        false,
		}

		if qp != nil {
			if key, pubkeyErr := types.TryPubkeyFromString(addr); pubkeyErr == nil {
				if price, ok := qp.Prices[key]; ok {
					priceItem.PriceUsd = price.Price
					priceItem.Decimals = uint32(price.Decimals)
					priceItem.Valid = true
				}
			} else {
				logger.Warnf("invalid quote address: %s, err: %v", addr, pubkeyErr)
			}
		}

		prices = append(prices, priceItem)
	}

	// 4. 返回结果
	return &pb.GetQuotePriceResponse{Prices: prices}, nil
}

func parseRankingKey(req *pb.GetRankingListRequest) (key types.RankingKey, filterFunc FilterFunc, err error) {
	// 1. Category 映射
	switch req.RankType {
	case 1:
		key.Category = types.RankingCategoryHot
	case 2:
		key.Category = types.RankingCategoryFull
	default:
		return key, nil, fmt.Errorf("invalid rank_type: %d", req.RankType)
	}

	// 2. Field 映射
	switch req.SortBy {
	// 全局字段
	case "liquidity":
		key.Field = types.RankingFieldLiquidity
	case "market", "market_cap":
		key.Field = types.RankingFieldMarketCap
	case "fdv":
		key.Field = types.RankingFieldFDV
	case "holder_count":
		key.Field = types.RankingFieldHolderCount
	case "top10_holders_rate":
		key.Field = types.RankingFieldTop10HolderRatio
	case "listing_time":
		key.Field = types.RankingFieldListingAt

	// 窗口字段
	case "volume":
		key.Field = types.RankingFieldVolume
	case "total_trades":
		key.Field = types.RankingFieldTxCount
	case "price_change":
		key.Field = types.RankingFieldPriceChange

	default:
		return key, nil, fmt.Errorf("invalid sort_by: %s", req.SortBy)
	}

	// 3. Hot 分类校验
	if key.Field > types.RankingFieldHotStart && key.Field < types.RankingFieldHotEnd {
		if key.Category != types.RankingCategoryHot {
			return key, nil, fmt.Errorf("field %v is only allowed for hot category", key.Field)
		}
	}

	// 4. Window 设置
	if key.Field > types.RankingFieldWindowStart && key.Field < types.RankingFieldWindowEnd {
		switch req.TimeFrame {
		case "1m":
			key.Window = types.Window1Min
		case "5m":
			key.Window = types.Window5Min
		case "1h":
			key.Window = types.Window1H
		case "6h":
			key.Window = types.Window6H
		case "24h":
			key.Window = types.Window24H
		default:
			return key, nil, fmt.Errorf("invalid time_frame: %s for field %v", req.TimeFrame, key.Field)
		}
	} else {
		key.Window = types.WindowGlobal
	}

	filterFunc = getFilterFunc(key, req.PositionDirection)
	return key, filterFunc, nil
}

func getFilterFunc(key types.RankingKey, positionDirection int32) FilterFunc {
	if key.Category != types.RankingCategoryHot {
		return nil
	}
	switch positionDirection {
	case 0:
		return filterLongAndShortLeverage
	case 1:
		return filterLongLeverage
	case 2:
		return filterShortLeverage
	case 3: // 多或空
		return nil
	default:
		return nil
	}
}

func filterLongAndShortLeverage(item types.RankingItem[*pool.Pool]) bool {
	return item.TickerData.TradeParams != nil &&
		item.TickerData.TradeParams.LongLeverage != 0 &&
		item.TickerData.TradeParams.ShortLeverage != 0
}

func filterLongLeverage(item types.RankingItem[*pool.Pool]) bool {
	return item.TickerData.TradeParams != nil && item.TickerData.TradeParams.LongLeverage != 0
}

func filterShortLeverage(item types.RankingItem[*pool.Pool]) bool {
	return item.TickerData.TradeParams != nil && item.TickerData.TradeParams.ShortLeverage != 0
}
