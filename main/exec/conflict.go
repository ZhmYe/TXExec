package exec

import (
	"fmt"
	"math/rand"
	"strconv"
)

func generateBlocks(PeerNumber int) []Block {
	var blocks = make([]Block, 0)
	for i := 0; i < PeerNumber; i++ {
		var txs = GenTxSet()
		block := NewBlock(txs)
		blocks = append(blocks, *block)
	}
	return blocks
}
func getFakeHashtable(block Block) map[string]StateSet {
	hashtable := make(map[string]StateSet)
	//length := 0
	for txIndex, tx := range block.txs {
		for _, op := range tx.Ops {
			_, ok := hashtable[op.Key]
			if !ok {
				hashtable[op.Key] = *newStateSet()
			}
			txHash := strconv.Itoa(rand.Intn(config.PeerNumber)) + "_" + strconv.Itoa(rand.Intn(10)) + "_" + strconv.Itoa(txIndex)
			unit := newUnit(op, tx, txHash)
			stateSet := hashtable[op.Key]
			if unit.op.Type == OpRead {
				stateSet.appendToReadSet(*unit)
			} else {
				stateSet.appendToWriteSet(*unit)
			}
		}
	}
	//for _, v := range hashtable {
	//	length += len(v)
	//}
	//fmt.Print(length)
	return hashtable
}

func solveConflict(blocks []Block) {
	hashTables := make([]map[string]StateSet, 0)
	lengthBeforeSolve := 0
	for _, block := range blocks {
		lengthBeforeSolve += len(block.txs) * config.OpsPerTx
		hashtable := getFakeHashtable(block)
		hashTables = append(hashTables, hashtable)
	}
	solutionByBaseLine := newSolution(hashTables)
	resultByBaseLine := solutionByBaseLine.getResult()
	lengthAfterSolveByBaseLine := 0
	for _, set := range resultByBaseLine {
		lengthAfterSolveByBaseLine += len(set.ReadSet)
		lengthAfterSolveByBaseLine += len(set.WriteSet)
	}
	lengthAfterSolveByBaseLine /= config.OpsPerTx
	//fmt.Println(getOpsNumber(hashTables[0]))
	//solutionByIndexChoose := newSolution(hashTables)
	////fmt.Println(getOpsNumber(solutionByIndexChoose.result))
	//resultByIndexChoose := solutionByIndexChoose.getResult(IndexChoose)
	//lengthAfterSolveByIndexChoose := 0
	//for _, value := range resultByIndexChoose {
	//	lengthAfterSolveByIndexChoose += len(value)
	//}
	fmt.Print("total txs: ")
	fmt.Println(lengthBeforeSolve)
	//fmt.Println(lengthAfterSolveByBaseLine)
	//fmt.Println(lengthAfterSolveByIndexChoose)
	fmt.Print("abort rate by baseline:")
	fmt.Println(1 - float64(lengthAfterSolveByBaseLine)/float64(lengthBeforeSolve))
	//fmt.Print("abort rate by IndexChoose:")
	//fmt.Println(1 - float64(lengthAfterSolveByIndexChoose)/float64(lengthBeforeSolve))
}
func Test() {
	blocks := generateBlocks(config.PeerNumber)
	solveConflict(blocks)
}
