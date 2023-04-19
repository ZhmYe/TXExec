package exec

//import (
//	"fmt"
//	"strconv"
//	"time"
//)
//
//func abortRateTest() {
//	peerId := generateIds(config.PeerNumber)
//	publicKey, hashed, signature := getSignInfo()
//	var timestamp = time.Now()
//	//var flag = false
//	saving, savingAmount := GenSaving(config.OriginKeys)
//	checking, checkingAmount := GenChecking(config.OriginKeys)
//	var peer = newPeer(peerId[0], Monitor, timestamp, peerId, saving, savingAmount, checking, checkingAmount, publicKey, hashed, signature)
//	execBlocks := make(map[int][]Block)
//	blockNumber := make([]int, 4)
//	blockNumber[0] = 1
//	blockNumber[1] = 1
//	blockNumber[2] = 1
//	blockNumber[3] = 1
//	totalNumber := blockNumber[0] + blockNumber[1] + blockNumber[2] + blockNumber[3]
//	for id, number := range blockNumber {
//		blocks := make([]Block, 0)
//		for i := 0; i < number; i++ {
//			var tx = peer.smallBank.GenTxSet(config.BatchTxNum)
//			//peer.log("generate tx:" + strconv.Itoa(len(tx)))
//			newBlock := NewBlock(tx)
//			blocks = append(blocks, *newBlock)
//		}
//		execBlocks[id] = blocks
//	}
//	fmt.Print("block number:" + strconv.Itoa(totalNumber))
//	startTime := time.Now()
//	peer.execInParalleling(execBlocks)
//	fmt.Print("exec time:")
//	fmt.Print(time.Since(startTime))
//	startTime = time.Now()
//	peer.OperationAfterExecution(instances)
//	fmt.Print(" abort time:")
//	fmt.Print(time.Since(startTime))
//	abortNumber := 0
//	for _, blocks := range execBlocks {
//		for i := 0; i < len(blocks); i++ {
//			for _, transaction := range blocks[i].txs {
//				if transaction.abort {
//					abortNumber += 1
//				}
//			}
//		}
//	}
//	fmt.Print(" abort rate:")
//	fmt.Println(float64(abortNumber) / float64(totalNumber*config.BatchTxNum))
//}
