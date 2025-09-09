package shared

import (
	"dex-stats-sol/internal/pkg/utils"
	ea "dex-stats-sol/internal/stats/eventadapter"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"sort"
)

const TopHoldersCapacity = 64
const maxAccounts = 50 // 限制最大账户数

// TopHolders 保存某个 token 的前 N 个账户余额信息（按余额倒序排序）
type TopHolders struct {
	accountIndexMap   map[types.Pubkey]uint16  // 用于存储账户的索引
	accounts          []*ea.AccountBalanceInfo // 按余额倒序排序的账户余额列表
	latestBlockNumber uint32                   // 最新的 BlockNumber
	needsSync         bool                     // 是否需要触发同步/修正
	nonPoolCount      uint16                   // topHolders 非池子账户总数
	top10Balance      utils.AtomicFloat64      // top10 非池子账户余额总和（float64，以 token 为单位）
}

func NewTopHolders(blockNumber uint32, isNewCreate bool) *TopHolders {
	th := &TopHolders{
		accountIndexMap: make(map[types.Pubkey]uint16, TopHoldersCapacity),
		accounts:        make([]*ea.AccountBalanceInfo, 0, TopHoldersCapacity),
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
		accountIndexMap: make(map[types.Pubkey]uint16),
		accounts:        make([]*ea.AccountBalanceInfo, 0),
		needsSync:       true,
	}
}

// IsEmptyInit 判断是否为初始空状态（未使用/未恢复）。
func (th *TopHolders) IsEmptyInit() bool {
	return th.latestBlockNumber == 0 &&
		th.needsSync &&
		cap(th.accounts) == 0 &&
		len(th.accountIndexMap) == 0
}

// InitIfEmpty 若处于初始空状态，则分配容量。
func (th *TopHolders) InitIfEmpty() bool {
	if th.IsEmptyInit() {
		th.accountIndexMap = make(map[types.Pubkey]uint16, TopHoldersCapacity)
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
	// 录入新数据
	utils.ClearSlice(&th.accounts)
	th.accounts = append(th.accounts, infos...)

	// 排序并重置索引
	th.sortAccounts()

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

	// 截断账户
	th.trimAccounts()

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
		// 如果账户余额小于最小余额
		if info.Balance < minBalance {
			// 如果账户在 Top 榜中, 则需把账户移除
			if index := th.findAccount(info); index >= 0 {
				if info.BlockNumber >= th.accounts[index].BlockNumber {
					th.removeAt(index)
					needsSort = true
				}
			}
			continue
		}

		if index := th.findAccount(info); index >= 0 {
			// 已在 Top 榜的账户，检查是否需要更新信息
			if info.BlockNumber >= th.accounts[index].BlockNumber {
				// 如果余额变化，标记需要排序
				if th.accounts[index].Balance != info.Balance {
					needsSort = true
				}
				th.accounts[index] = info // 更新账户信息
			}
			continue
		}

		// 新插入一条数据
		if info.BlockNumber >= th.latestBlockNumber {
			th.accounts = append(th.accounts, info)
			th.accountIndexMap[info.Account] = uint16(len(th.accounts) - 1)
			if info.Balance > minBalance {
				needsSort = true
			}
		}
		continue
	}

	// 更新 blockNumber
	th.latestBlockNumber = max(th.latestBlockNumber, blockNumber)

	// 排序
	if needsSort {
		th.sortAccounts()
	}

	// 更新 top10 和非池子数量
	top10Balance, nonPoolCount := th.summarizeTop10NonPool(top10)
	top10Changed = th.Top10Balance() != top10Balance
	if top10Changed {
		th.top10Balance.Store(top10Balance)
	}

	// 判断是否需要重新同步
	if nonPoolCount < minNonPoolForSync && uint16(nonPoolCount) < th.nonPoolCount {
		th.needsSync = true
	}
	th.nonPoolCount = uint16(nonPoolCount)
	syncRequired = th.needsSync && !oldNeedsSync

	// 截断账户
	th.trimAccounts()
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
	if index, exists := th.accountIndexMap[info.Account]; exists {
		return int(index)
	}
	return -1
}

// removeAt 从 accounts 和 accountIndexMap 中移除指定索引的账户。
// 通过交换位置减少删除开销
func (th *TopHolders) removeAt(index int) {
	if index < 0 || index >= len(th.accounts) {
		return
	}

	removedAcc := th.accounts[index]
	lastIdx := len(th.accounts) - 1

	// 如果不是最后一个账户，将其替换为最后一个账户
	if index != lastIdx {
		th.accounts[index] = th.accounts[lastIdx]
		th.accountIndexMap[th.accounts[lastIdx].Account] = uint16(index)
	}

	// 清除引用，删除最后一个账户
	th.accounts[lastIdx] = nil
	th.accounts = th.accounts[:lastIdx]

	// 从 accountIndexMap 中移除该账户
	delete(th.accountIndexMap, removedAcc.Account)
}

// sortAccounts 对账户进行排序并重置索引
func (th *TopHolders) sortAccounts() {
	sort.Slice(th.accounts, func(i, j int) bool {
		return th.accounts[i].Balance > th.accounts[j].Balance
	})

	// 更新 accountIndexMap 索引
	clear(th.accountIndexMap)
	for i, acc := range th.accounts {
		th.accountIndexMap[acc.Account] = uint16(i)
	}
}

// trimAccounts 截断账户列表，确保不超过最大账户数量
func (th *TopHolders) trimAccounts() {
	if len(th.accounts) <= maxAccounts {
		return // 不需要截断，直接返回
	}

	// 删除超出部分的账户，并更新 accountIndexMap
	for i := maxAccounts; i < len(th.accounts); i++ {
		accountToRemove := th.accounts[i].Account
		delete(th.accountIndexMap, accountToRemove) // 删除 accountIndexMap 中的账户
	}

	// 截断 accounts 列表
	clear(th.accounts[maxAccounts:]) // 清空引用
	th.accounts = th.accounts[:maxAccounts]
}

// ToProto 将 TopHolders 转换成 Protobuf Snapshot
func (th *TopHolders) ToProto() *pb.TopHoldersSnapshot {
	accounts := make([]*pb.AccountBalanceSnapshot, 0, len(th.accounts))
	for _, a := range th.accounts {
		accounts = append(accounts, &pb.AccountBalanceSnapshot{
			Holder:        a.Account[:],
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
		accountIndexMap:   make(map[types.Pubkey]uint16, TopHoldersCapacity),
		accounts:          make([]*ea.AccountBalanceInfo, 0, TopHoldersCapacity),
		latestBlockNumber: p.LatestBlockNumber,
		needsSync:         p.NeedsSync,
	}

	for _, a := range p.Accounts {
		var account types.Pubkey
		copy(account[:], a.Holder)
		th.accounts = append(th.accounts, &ea.AccountBalanceInfo{
			Account:       account,
			Balance:       a.Balance,
			BlockNumber:   a.BlockNumber,
			IsPoolAccount: a.IsPoolAccount,
		})
	}
	th.sortAccounts()
	th.trimAccounts()

	// 从 accounts 重新计算 top10Balance 和 nonPoolCount
	top10Balance, nonPoolCount := th.summarizeTop10NonPool(10)
	th.top10Balance.Store(top10Balance)
	th.nonPoolCount = uint16(nonPoolCount)

	return th
}
