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
func getFakeHashtable(block Block) map[string][]Unit {
	hashtable := make(map[string][]Unit)
	//length := 0
	for txIndex, tx := range block.txs {
		for _, op := range tx.Ops {
			if hashtable[op.Key] == nil {
				hashtable[op.Key] = make([]Unit, 0)
			}
			txHash := strconv.Itoa(rand.Intn(config.PeerNumber)) + "_" + strconv.Itoa(rand.Intn(10)) + "_" + strconv.Itoa(txIndex)
			unit := newUnit(op, txHash)
			hashtable[op.Key] = append(hashtable[op.Key], *unit)
		}
	}
	//for _, v := range hashtable {
	//	length += len(v)
	//}
	//fmt.Print(length)
	return hashtable
}

func solveConflict(blocks []Block) {
	hashTables := make([]map[string][]Unit, 0)
	lengthBeforeSolve := 0
	for _, block := range blocks {
		lengthBeforeSolve += len(block.txs) * config.OpsPerTx
		hashtable := getFakeHashtable(block)
		hashTables = append(hashTables, hashtable)
	}
	solutionByBaseLine := newSolution(hashTables)
	resultByBaseLine := solutionByBaseLine.getResult()
	lengthAfterSolveByBaseLine := 0
	for _, value := range resultByBaseLine {
		lengthAfterSolveByBaseLine += len(value)
	}
	//fmt.Println(getOpsNumber(hashTables[0]))
	//solutionByIndexChoose := newSolution(hashTables)
	////fmt.Println(getOpsNumber(solutionByIndexChoose.result))
	//resultByIndexChoose := solutionByIndexChoose.getResult(IndexChoose)
	//lengthAfterSolveByIndexChoose := 0
	//for _, value := range resultByIndexChoose {
	//	lengthAfterSolveByIndexChoose += len(value)
	//}
	fmt.Print("total ops: ")
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
