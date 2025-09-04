package shared

import (
	"dex-stats-sol/internal/pkg/utils"
	ea "dex-stats-sol/internal/stats/eventadapter"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"sort"
)

const TopHoldersCapacity = 64

// TopHolders 保存某个 token 的前 N 个账户余额信息（按余额倒序排序）
type TopHolders struct {
	accountSet        map[types.Pubkey]struct{} // account 集合，用于快速判断账户是否已存在
	accounts          []*ea.AccountBalanceInfo  // 按余额倒序排序的账户余额列表
	latestBlockNumber uint32                    // 最新的 BlockNumber
	needsSync         bool                      // 是否需要触发同步/修正
	nonPoolCount      uint16                    // topHolders 非池子账户总数
	top10Balance      utils.AtomicFloat64       // top10 非池子账户余额总和（float64，以 token 为单位）
}

func NewTopHolders(blockNumber uint32, isNewCreate bool) *TopHolders {
	th := &TopHolders{
		accountSet: make(map[types.Pubkey]struct{}, TopHoldersCapacity),
		accounts:   make([]*ea.AccountBalanceInfo, 0, TopHoldersCapacity),
	}

	if isNewCreate {
		th.latestBlockNumber = blockNumber
		th.needsSync = false
	} else {
		th.latestBlockNumber = 0
		th.needsSync = true
	}

	return th
}

// NewEmptyTopHolders 创建初始空 TopHolders，未分配容量。
func NewEmptyTopHolders() *TopHolders {
	return &TopHolders{
		accountSet: make(map[types.Pubkey]struct{}),
		accounts:   make([]*ea.AccountBalanceInfo, 0),
		needsSync:  true,
	}
}

// IsEmptyInit 判断是否为初始空状态（未使用/未恢复）。
func (th *TopHolders) IsEmptyInit() bool {
	return th.latestBlockNumber == 0 &&
		th.needsSync &&
		cap(th.accounts) == 0 &&
		len(th.accountSet) == 0
}

// InitIfEmpty 若处于初始空状态，则分配容量。
func (th *TopHolders) InitIfEmpty() bool {
	if th.IsEmptyInit() {
		th.accountSet = make(map[types.Pubkey]struct{}, TopHoldersCapacity)
		th.accounts = make([]*ea.AccountBalanceInfo, 0, TopHoldersCapacity)
		return true
	}
	return false
}

// ShouldRequest 报告是否需要对 TopHolders 执行全量同步。
func (th *TopHolders) ShouldRequest() bool {
	return th.needsSync
}

// SyncTopHolders 使用全量账户信息同步 TopHolders。
// 返回 top10 非池子账户余额是否发生变化。
func (th *TopHolders) SyncTopHolders(infos []*ea.AccountBalanceInfo) bool {
	// 清空原有数据
	clear(th.accountSet)
	utils.ClearSlice(&th.accounts)

	for _, info := range infos {
		th.accountSet[info.Account] = struct{}{}
	}
	th.accounts = append(th.accounts, infos...)

	// 排序
	sort.Slice(th.accounts, func(i, j int) bool {
		return th.accounts[i].Balance > th.accounts[j].Balance
	})

	// 更新 blockNumber
	maxBlockNumber := uint32(0)
	for _, info := range infos {
		if info.BlockNumber > maxBlockNumber {
			maxBlockNumber = info.BlockNumber
		}
	}
	th.latestBlockNumber = maxBlockNumber

	// 统计 top10 非池子账户
	top10Balance, nonPoolCount := th.summarizeTop10NonPool(10)
	top10Changed := th.Top10Balance() != top10Balance
	th.top10Balance.Store(top10Balance)
	th.nonPoolCount = uint16(nonPoolCount)

	th.needsSync = false
	return top10Changed
}

// UpdateTopHolders 更新 top holders。
// 返回 top10 是否变化，以及是否需要触发同步。
func (th *TopHolders) UpdateTopHolders(blockNumber uint32, infos []*ea.AccountBalanceInfo) (top10Changed, syncRequired bool) {
	if len(infos) == 0 {
		return
	}

	const top10 = 10
	const minNonPoolForSync = 15

	oldNeedsSync := th.needsSync
	minBalance := th.getMinBalance()
	needsSort := false

	// 合并新账户信息
	for _, info := range infos {
		index := th.findAccount(info)

		switch {
		// 余额低于最小值，移除
		case info.Balance < minBalance:
			if index != -1 && info.BlockNumber >= th.accounts[index].BlockNumber {
				th.removeAt(index)
				needsSort = true
			}

		// 进入 top holders 的新账户
		case index == -1:
			if info.BlockNumber >= th.latestBlockNumber {
				// 最新 block 新账户，加入 topHolders
				th.accounts = append(th.accounts, info)
				th.accountSet[info.Account] = struct{}{}
				if info.Balance > minBalance {
					needsSort = true
				}
			}

		// 已存在账户，更新信息
		default:
			if info.BlockNumber >= th.accounts[index].BlockNumber {
				if th.accounts[index].Balance != info.Balance {
					needsSort = true
				}
				th.accounts[index] = info
			}
		}
	}

	// 更新 blockNumber
	th.latestBlockNumber = max(th.latestBlockNumber, blockNumber)

	// 排序
	if needsSort {
		sort.Slice(th.accounts, func(i, j int) bool {
			return th.accounts[i].Balance > th.accounts[j].Balance
		})
	}

	// 更新 top10 和非池子数量
	top10Balance, nonPoolCount := th.summarizeTop10NonPool(top10)
	top10Changed = th.Top10Balance() != top10Balance
	if top10Changed {
		th.top10Balance.Store(top10Balance)
	}

	// 判断是否需要重新同步
	if nonPoolCount < minNonPoolForSync && th.nonPoolCount > uint16(nonPoolCount) {
		th.needsSync = true
	}
	th.nonPoolCount = uint16(nonPoolCount)
	syncRequired = th.needsSync && !oldNeedsSync
	return
}

// Top10Balance 返回当前 top10 非池子账户余额总和。
func (th *TopHolders) Top10Balance() float64 {
	return th.top10Balance.Load()
}

// summarizeTop10NonPool 返回 top10 非池子账户余额总和和非池子账户总个数。
func (th *TopHolders) summarizeTop10NonPool(top int) (top10Balance float64, nonPoolCount int) {
	for _, h := range th.accounts {
		if h.IsPoolAccount {
			continue
		}
		if nonPoolCount < top {
			top10Balance += h.Balance
		}
		nonPoolCount++
	}
	return
}

// findSafeBalanceCutoff 返回第一个低于 safeBalance 的位置，以及该位置之前非池子账户总数
func (th *TopHolders) findSafeBalanceCutoff(safeBalance float64) (pos int, nonPoolCount int) {
	pos = len(th.accounts) // 默认不截断
	nonPoolCount = 0

	for i, h := range th.accounts {
		if h.Balance < safeBalance {
			pos = i
			break
		}
		if !h.IsPoolAccount {
			nonPoolCount++
		}
	}
	return
}

// getMinBalance 返回当前 topHolders 最小余额
func (th *TopHolders) getMinBalance() float64 {
	n := len(th.accounts)
	if n == 0 {
		return 0
	}
	return th.accounts[n-1].Balance
}

// findAccount 查找账户在 accounts 切片中的索引。
func (th *TopHolders) findAccount(info *ea.AccountBalanceInfo) int {
	if _, exists := th.accountSet[info.Account]; !exists {
		return -1
	}

	for i, item := range th.accounts {
		if item.Account == info.Account {
			return i
		}
	}
	return -1
}

// removeAt 从 accounts 和 accountSet 中移除指定索引的账户。
func (th *TopHolders) removeAt(index int) {
	if index < 0 || index >= len(th.accounts) {
		return
	}

	acc := th.accounts[index]
	last := len(th.accounts) - 1
	if index != last {
		th.accounts[index] = th.accounts[last]
	}
	th.accounts[last] = nil // 清除引用
	th.accounts = th.accounts[:last]
	delete(th.accountSet, acc.Account)
}

// ToProto 将 TopHolders 转换成 Protobuf Snapshot
func (th *TopHolders) ToProto() *pb.TopHoldersSnapshot {
	accounts := make([]*pb.AccountBalanceSnapshot, 0, len(th.accounts))
	for _, a := range th.accounts {
		accounts = append(accounts, &pb.AccountBalanceSnapshot{
			Owner:         a.Account[:],
			Balance:       a.Balance,
			BlockNumber:   a.BlockNumber,
			IsPoolAccount: a.IsPoolAccount,
		})
	}

	return &pb.TopHoldersSnapshot{
		Accounts:          accounts,
		LatestBlockNumber: th.latestBlockNumber,
		NeedsSync:         th.needsSync,
	}
}

// NewTopHoldersFromProto 根据 Protobuf Snapshot 新建一个 TopHolders
func NewTopHoldersFromProto(p *pb.TopHoldersSnapshot) *TopHolders {
	th := &TopHolders{
		accountSet:        make(map[types.Pubkey]struct{}, TopHoldersCapacity),
		accounts:          make([]*ea.AccountBalanceInfo, 0, TopHoldersCapacity),
		latestBlockNumber: p.LatestBlockNumber,
		needsSync:         p.NeedsSync,
	}

	for _, a := range p.Accounts {
		var owner types.Pubkey
		copy(owner[:], a.Owner)
		info := &ea.AccountBalanceInfo{
			Account:       owner,
			Balance:       a.Balance,
			BlockNumber:   a.BlockNumber,
			IsPoolAccount: a.IsPoolAccount,
		}
		th.accounts = append(th.accounts, info)
		th.accountSet[owner] = struct{}{}
	}

	// 从 accounts 重新计算 top10Balance 和 nonPoolCount
	top10Balance, nonPoolCount := th.summarizeTop10NonPool(10)
	th.top10Balance.Store(top10Balance)
	th.nonPoolCount = uint16(nonPoolCount)

	return th
}
