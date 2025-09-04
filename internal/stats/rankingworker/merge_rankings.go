package rankingworker

import (
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/types"
)

// 硬编码合并 1~6 路列表，实测性能最佳。
// 硬编码可能利用 CPU 缓存优化，为了极致性能采用此方式。
// 超过 6 路则使用通用 k-way merge。
func mergeSortedRankings(cmp CompareFunc, lists [][]types.RankingItem[*pool.Pool]) []types.RankingItem[*pool.Pool] {
	switch len(lists) {
	case 0:
		return nil
	case 1:
		return lists[0]
	case 2:
		return merge2Extreme(cmp, lists[0], lists[1])
	case 3:
		return merge3Extreme(cmp, lists[0], lists[1], lists[2])
	case 4:
		return merge4Extreme(cmp, lists[0], lists[1], lists[2], lists[3])
	case 5:
		return merge5Extreme(cmp, lists[0], lists[1], lists[2], lists[3], lists[4])
	case 6:
		return merge6Extreme(cmp, lists[0], lists[1], lists[2], lists[3], lists[4], lists[5])
	default:
		return kWayMerge(cmp, lists)
	}
}

// merge2Extreme 硬编码 2 路
func merge2Extreme(cmp CompareFunc, a, b []types.RankingItem[*pool.Pool]) []types.RankingItem[*pool.Pool] {
	la, lb := len(a), len(b)
	total := la + lb
	res := make([]types.RankingItem[*pool.Pool], total)

	ia, ib, idx := 0, 0, 0
	for ; ia < la && ib < lb; idx++ {
		if cmp(b[ib], a[ia]) {
			res[idx] = b[ib]
			ib++
		} else {
			res[idx] = a[ia]
			ia++
		}
	}

	if ia < la {
		copy(res[idx:], a[ia:])
	} else if ib < lb {
		copy(res[idx:], b[ib:])
	}
	return res
}

// merge3Extreme 硬编码 3 路
func merge3Extreme(cmp CompareFunc, a, b, c []types.RankingItem[*pool.Pool]) []types.RankingItem[*pool.Pool] {
	la, lb, lc := len(a), len(b), len(c)
	total := la + lb + lc
	res := make([]types.RankingItem[*pool.Pool], total)

	var winner types.RankingItem[*pool.Pool]
	ia, ib, ic := 0, 0, 0
	for idx := 0; ; idx++ {
		src := 0
		if ia < la {
			winner = a[ia]
			src = 1
		}
		if ib < lb && (src == 0 || cmp(b[ib], winner)) {
			winner = b[ib]
			src = 2
		}
		if ic < lc && (src == 0 || cmp(c[ic], winner)) {
			winner = c[ic]
			src = 3
		}

		switch src {
		case 1:
			ia++
		case 2:
			ib++
		case 3:
			ic++
		case 0:
			return res
		}
		res[idx] = winner
	}
}

// merge4Extreme 硬编码 4 路
func merge4Extreme(cmp CompareFunc, a, b, c, d []types.RankingItem[*pool.Pool]) []types.RankingItem[*pool.Pool] {
	la, lb, lc, ld := len(a), len(b), len(c), len(d)
	total := la + lb + lc + ld
	res := make([]types.RankingItem[*pool.Pool], total)

	var winner types.RankingItem[*pool.Pool]
	ia, ib, ic, id := 0, 0, 0, 0
	for idx := 0; ; idx++ {
		src := 0
		if ia < la {
			winner = a[ia]
			src = 1
		}
		if ib < lb && (src == 0 || cmp(b[ib], winner)) {
			winner = b[ib]
			src = 2
		}
		if ic < lc && (src == 0 || cmp(c[ic], winner)) {
			winner = c[ic]
			src = 3
		}
		if id < ld && (src == 0 || cmp(d[id], winner)) {
			winner = d[id]
			src = 4
		}

		switch src {
		case 1:
			ia++
		case 2:
			ib++
		case 3:
			ic++
		case 4:
			id++
		case 0:
			return res
		}
		res[idx] = winner
	}
}

// merge5Extreme 硬编码 5 路
func merge5Extreme(cmp CompareFunc, a, b, c, d, e []types.RankingItem[*pool.Pool]) []types.RankingItem[*pool.Pool] {
	la, lb, lc, ld, le := len(a), len(b), len(c), len(d), len(e)
	total := la + lb + lc + ld + le
	res := make([]types.RankingItem[*pool.Pool], total)

	var winner types.RankingItem[*pool.Pool]
	ia, ib, ic, id, ie := 0, 0, 0, 0, 0
	for idx := 0; ; idx++ {
		src := 0
		if ia < la {
			winner = a[ia]
			src = 1
		}
		if ib < lb && (src == 0 || cmp(b[ib], winner)) {
			winner = b[ib]
			src = 2
		}
		if ic < lc && (src == 0 || cmp(c[ic], winner)) {
			winner = c[ic]
			src = 3
		}
		if id < ld && (src == 0 || cmp(d[id], winner)) {
			winner = d[id]
			src = 4
		}
		if ie < le && (src == 0 || cmp(e[ie], winner)) {
			winner = e[ie]
			src = 5
		}

		switch src {
		case 1:
			ia++
		case 2:
			ib++
		case 3:
			ic++
		case 4:
			id++
		case 5:
			ie++
		case 0:
			return res
		}
		res[idx] = winner
	}
}

// merge6Extreme 硬编码 6 路
func merge6Extreme(cmp CompareFunc, a, b, c, d, e, f []types.RankingItem[*pool.Pool]) []types.RankingItem[*pool.Pool] {
	la, lb, lc, ld, le, lf := len(a), len(b), len(c), len(d), len(e), len(f)
	total := la + lb + lc + ld + le + lf
	res := make([]types.RankingItem[*pool.Pool], total)

	var winner types.RankingItem[*pool.Pool]
	ia, ib, ic, id, ie, iF := 0, 0, 0, 0, 0, 0

	for idx := 0; ; idx++ {
		src := 0
		if ia < la {
			winner = a[ia]
			src = 1
		}
		if ib < lb && (src == 0 || cmp(b[ib], winner)) {
			winner = b[ib]
			src = 2
		}
		if ic < lc && (src == 0 || cmp(c[ic], winner)) {
			winner = c[ic]
			src = 3
		}
		if id < ld && (src == 0 || cmp(d[id], winner)) {
			winner = d[id]
			src = 4
		}
		if ie < le && (src == 0 || cmp(e[ie], winner)) {
			winner = e[ie]
			src = 5
		}
		if iF < lf && (src == 0 || cmp(f[iF], winner)) {
			winner = f[iF]
			src = 6
		}

		switch src {
		case 1:
			ia++
		case 2:
			ib++
		case 3:
			ic++
		case 4:
			id++
		case 5:
			ie++
		case 6:
			iF++
		case 0:
			return res
		}

		res[idx] = winner
	}
}

// 通用 k-way merge (>6 路)
func kWayMerge(cmp CompareFunc, lists [][]types.RankingItem[*pool.Pool]) []types.RankingItem[*pool.Pool] {
	total := 0
	for _, l := range lists {
		total += len(l)
	}

	res := make([]types.RankingItem[*pool.Pool], total)
	K := len(lists)
	indexes := make([]int, K)
	var winner types.RankingItem[*pool.Pool]

	for idx := 0; idx < total; idx++ {
		src := -1
		for i := 0; i < K; i++ {
			if indexes[i] < len(lists[i]) {
				item := lists[i][indexes[i]]
				if src == -1 || cmp(item, winner) {
					winner = item
					src = i
				}
			}
		}

		res[idx] = winner
		indexes[src]++
	}

	return res
}
