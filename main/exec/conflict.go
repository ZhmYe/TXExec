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
		flag := false
		tmpAddress := make([]string, 0)
		tmpRecord := make(map[string]bool)
		for _, op := range tx.Ops {
			_, haveRecord := tmpRecord[op.Key]
			if !haveRecord {
				tmpRecord[op.Key] = true
				tmpAddress = append(tmpAddress, op.Key)
			}
		}
		for _, address := range tmpAddress {
			_, exist := addressA[address]
			if exist {
				flag = true
				continue
			}
			addressA[address] = true
		}
		if flag {
			innerConflictNumber++
		}
	}
	fmt.Print("conflict rate in blocks:")
	fmt.Println(float64(innerConflictNumber) / float64(config.BatchTxNum))
	conflictNumber := 0
	for _, tx := range txsB {
		flag := false
		for _, op := range tx.Ops {
			if addressA[op.Key] {
				flag = true
			}
		}
		if flag {
			conflictNumber++
		}
	}
	fmt.Print("conflict rate between blocks: ")
	fmt.Println(float64(conflictNumber) / float64(config.BatchTxNum))
}
