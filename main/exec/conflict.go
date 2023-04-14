package exec

import (
	"fmt"
)

func ConflictTest() {
	smallBank := NewSmallbank("testConflict", config.OriginKeys)
	txsA := smallBank.GenTxSet(config.BatchTxNum)
	txsB := smallBank.GenTxSet(config.BatchTxNum)
	addressA := make(map[string]bool)
	innerConflictNumber := 0
	for _, tx := range txsA {
		for _, op := range tx.Ops {
			_, exist := addressA[op.Key]
			if exist {
				innerConflictNumber++
				continue
			}
			addressA[op.Key] = true
		}
	}
	fmt.Print("conflict rate in blocks:")
	fmt.Println(float64(innerConflictNumber) / float64(config.BatchTxNum))
	conflictNumber := 0
	for _, tx := range txsB {
		for _, op := range tx.Ops {
			if addressA[op.Key] {
				conflictNumber++
			}
		}
	}
	fmt.Print("conflict rate between blocks: ")
	fmt.Println(float64(conflictNumber) / float64(config.BatchTxNum))
}
