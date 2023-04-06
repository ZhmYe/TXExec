package exec

import "sort"

type OrderTxs struct {
	order int  // 排序序列号
	txs   []Tx // 这一序列运行的交易
}

func newOrderTxs(order int) *OrderTxs {
	orderTxs := new(OrderTxs)
	orderTxs.order = order
	orderTxs.txs = make([]Tx, 0)
	return orderTxs
}
func (orderTxs *OrderTxs) appendTx(tx Tx) {
	orderTxs.txs = append(orderTxs.txs, tx)
}
func getAllOrderTxs(blocks []Block) []OrderTxs {
	tmpMap := make(map[int]OrderTxs, 0)
	for _, block := range blocks {
		for _, tx := range block.txs {
			if tx.abort {
				continue
			}
			_, exist := tmpMap[tx.sequence]
			if !exist {
				tmpMap[tx.sequence] = *newOrderTxs(tx.sequence)
			} else {
				orderTxs := tmpMap[tx.sequence]
				orderTxs.appendTx(*tx)
				tmpMap[tx.sequence] = orderTxs
			}
		}
	}
	sortedKeys := make([]int, 0)
	for key, _ := range tmpMap {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Ints(sortedKeys)
	result := make([]OrderTxs, 0)
	for _, key := range sortedKeys {
		result = append(result, tmpMap[key])
	}
	return result
}
func getMinSeq(sortedRSet []Unit) int {
	minSeq := 100000000
	for _, unit := range sortedRSet {
		if unit.tx.sequence < minSeq {
			minSeq = unit.tx.sequence
		}
	}
	return minSeq
}
func getMaxSeq(sortedRSet []Unit) int {
	maxSeq := -1
	for _, unit := range sortedRSet {
		if unit.tx.sequence > maxSeq {
			maxSeq = unit.tx.sequence
		}
	}
	return maxSeq
}
func getSortedRSet(Rw StateSet) []Unit {
	sortedRSet := make([]Unit, 0)
	for _, unit := range Rw.ReadSet {
		if unit.tx.sequence != -1 {
			sortedRSet = append(sortedRSet, unit)
		}
	}
	return sortedRSet
}
func getSortedWSet(Rw StateSet) []Unit {
	sortedWSet := make([]Unit, 0)
	for _, unit := range Rw.WriteSet {
		if unit.tx.sequence != -1 {
			sortedWSet = append(sortedWSet, unit)
		}
	}
	return sortedWSet
}
func TransactionSort(hashtable map[string]StateSet) {
	initialSeq := 0
	for _, Rw := range hashtable {
		maxRead := -1
		writeSeq := -1
		sortedRSet := getSortedRSet(Rw)
		ReadSetTxHash := make(map[string]bool, 0) // 用于判断是否有同意交易的读写在同一个key上
		// line 4 - 15
		if len(sortedRSet) == 0 {
			for _, unit := range Rw.ReadSet {
				unit.tx.sequence = initialSeq
				sortedRSet = append(sortedRSet, unit)
				_, exist := ReadSetTxHash[unit.txHash]
				if !exist {
					ReadSetTxHash[unit.txHash] = true
				}
			}
			maxRead = initialSeq
		} else {
			minSeq := getMinSeq(sortedRSet)
			maxSeq := getMaxSeq(sortedRSet)
			maxRead = maxSeq
			for _, unit := range Rw.ReadSet {
				if unit.tx.sequence == -1 {
					unit.tx.sequence = minSeq
					sortedRSet = append(sortedRSet, unit)
				}
				_, exist := ReadSetTxHash[unit.txHash]
				if !exist {
					ReadSetTxHash[unit.txHash] = true
				}
			}
		}
		// line 16 - 19
		sortedWSet := getSortedWSet(Rw)
		for _, unit := range sortedWSet {
			_, exist := ReadSetTxHash[unit.txHash]
			if exist {
				unit.tx.sequence = maxRead + 1
				maxRead += 1
			}
		}
		// line 20 - 24
		for _, unit := range sortedWSet {
			if unit.tx.sequence < maxRead {
				unit.tx.abort = true
			}
		}
		// line 25 - 29
		if len(Rw.ReadSet) == 0 {
			writeSeq = initialSeq
		} else {
			writeSeq = maxRead + 1
		}
		// line 30 - 35
		for _, unit := range Rw.WriteSet {
			if unit.tx.sequence == -1 {
				unit.tx.sequence = writeSeq
				writeSeq += 1
			}
		}
	}
}
