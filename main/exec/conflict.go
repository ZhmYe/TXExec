package exec

import "fmt"

func generateBlocks(PeerNumber int) []Block {
	var blocks = make([]Block, 0)
	for i := 0; i < PeerNumber; i++ {
		var txs = GenTxSet()
		block := NewBlock(txs)
		blocks = append(blocks, *block)
	}
	return blocks
}
func getFakeHashtable(block Block) map[string][]Op {
	hashtable := make(map[string][]Op)
	for _, tx := range block.txs {
		for _, op := range tx.Ops {
			if hashtable[op.Key] == nil {
				hashtable[op.Key] = make([]Op, 0)
			}
			hashtable[op.Key] = append(hashtable[op.Key], op)
		}
	}
	return hashtable
}

func solveConflict(blocks []Block) {
	hashTables := make([]map[string][]Op, 0)
	lengthBeforeSolve := 0
	for _, block := range blocks {
		lengthBeforeSolve += len(block.txs) * config.OpsPerTx
		hashtable := getFakeHashtable(block)
		hashTables = append(hashTables, hashtable)
	}
	solution := newSolution(hashTables...)
	resultByBaseLine := solution.getResult(Baseline)
	lengthAfterSolveByBaseLine := 0
	for _, value := range resultByBaseLine {
		lengthAfterSolveByBaseLine += len(value)
	}
	resultByIndexChoose := solution.getResult(IndexChoose)
	lengthAfterSolveByIndexChoose := 0
	for _, value := range resultByIndexChoose {
		lengthAfterSolveByIndexChoose += len(value)
	}
	fmt.Println(lengthBeforeSolve)
	fmt.Println(lengthAfterSolveByBaseLine)
	fmt.Println(lengthAfterSolveByIndexChoose)
	fmt.Print("abort rate by baseline:")
	fmt.Println(float64(lengthAfterSolveByBaseLine) / float64(lengthBeforeSolve))
	fmt.Print("abort rate by IndexChoose:")
	fmt.Println(float64(lengthAfterSolveByIndexChoose) / float64(lengthBeforeSolve))
}
func Test() {
	blocks := generateBlocks(config.PeerNumber)
	solveConflict(blocks)
}
