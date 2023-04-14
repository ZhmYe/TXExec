package exec

import "fmt"

func ConflictTest() {
	smallBank := NewSmallbank("testConflict", config.OriginKeys)
	txsA := smallBank.GenTxSet(config.BatchTxNum)
	txsB := smallBank.GenTxSet(config.BatchTxNum)
	addressA := make(map[string]bool)
	for _, tx := range txsA {
		for _, op := range tx.Ops {
			addressA[op.Key] = true
		}
	}
	conflictNumber := 0
	for _, tx := range txsB {
		for _, op := range tx.Ops {
			if addressA[op.Key] {
				conflictNumber++
			}
		}
	}
	fmt.Println(float64(conflictNumber) / float64(config.BatchTxNum))
}
