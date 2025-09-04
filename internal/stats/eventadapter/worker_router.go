package eventadapter

import (
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/pb"
	"fmt"
)

// RouteEventsToWorkers 按 pool 地址将事件分发到指定的 worker。
//   - events: 待分发的原始事件列表
//   - workers: 参与分发的 worker ID 列表
//   - workerCount: worker 总数量，用于哈希取模分配
//
// 返回值 map[workerID]*pb.Events，包含每个 worker 应处理的事件集合。
func RouteEventsToWorkers(events *pb.Events, workers []uint8, workerCount uint8) map[uint8]*pb.Events {
	routes := make(map[uint8]*pb.Events, len(workers))
	for _, workerID := range workers {
		routes[workerID] = &pb.Events{
			Version:     events.Version,
			ChainId:     events.ChainId,
			BlockNumber: events.BlockNumber,
			Source:      events.Source,
			QuotePrices: events.QuotePrices,
			BlockHash:   events.BlockHash,
			BlockTime:   events.BlockTime,
			Events:      make([]*pb.Event, 0, len(events.Events)),
		}
	}

	for _, event := range events.Events {
		poolAddr, keep := extractPoolAddress(event)
		if !keep {
			continue
		}

		if poolAddr == nil {
			for _, e := range routes {
				e.Events = append(e.Events, event)
			}
			continue
		}

		workerID := uint8(utils.PartitionHash(poolAddr) % uint32(workerCount))
		if _, ok := routes[workerID]; !ok {
			panic(fmt.Sprintf(
				"[RouteEventsToWorkers] invalid workerID=%d, poolAddr=%x, workers=%v, workerCount=%d",
				workerID, poolAddr, workers, workerCount,
			))
		}
		routes[workerID].Events = append(routes[workerID].Events, event)
	}

	return routes
}

func extractPoolAddress(e *pb.Event) ([]byte, bool) {
	switch evt := e.Event.(type) {
	case *pb.Event_Trade:
		return evt.Trade.PairAddress, true
	case *pb.Event_Liquidity:
		return evt.Liquidity.PairAddress, true
	case *pb.Event_Token:
		return evt.Token.PairAddress, true
	case *pb.Event_Transfer:
		return nil, false
	default:
		return nil, true
	}
}
