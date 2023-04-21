package exec

import (
	"fmt"
	"strconv"
	"time"
)

func SimpleTest() {
	peerId := generateIds(config.PeerNumber)
	publicKey, hashed, signature := getSignInfo()
	var timestamp = time.Now()
	//var flag = false
	saving, savingAmount := GenSaving(config.OriginKeys)
	checking, checkingAmount := GenChecking(config.OriginKeys)
	var peer = newPeer(peerId[0], Monitor, timestamp, peerId, saving, savingAmount, checking, checkingAmount, publicKey, hashed, signature)
	blockNumber := make([]int, 4)
	blockNumber[0] = 1
	blockNumber[1] = 1
	blockNumber[2] = 1
	blockNumber[3] = 1
	totalNumber := blockNumber[0] + blockNumber[1] + blockNumber[2] + blockNumber[3]
	for id, number := range blockNumber {
		blocks := make([]Block, 0)
		for i := 0; i < number; i++ {
			var tx = peer.smallBank.GenTxSet(config.BatchTxNum)
			//peer.log("generate tx:" + strconv.Itoa(len(tx)))
			newBlock := NewBlock(tx)
			blocks = append(blocks, *newBlock)
		}
		record := newRecord(id)
		record.index = 0
		record.blocks = blocks
		peer.record[id] = record
	}
	blocks4SequentialPlus := make([]Block, 0)
	for id, bias := range blockNumber {
		record := peer.record[id]
		blocks4SequentialPlus = append(blocks4SequentialPlus, record.blocks[record.index:record.index+bias]...)
	}
	//instances := make([]Instance, 0)
	//execBlocks := make(map[int][]Block, 0)
	//epoch := blockNumber
	//for id, bias := range epoch {
	//	record := peer.record[id]
	//	execBlocks[id] = record.blocks[record.index : record.index+bias]
	//	instance := newInstance(id)
	//	for i := 0; i < bias; i++ {
	//		hashtable := peer.getHashTable(id, i)
	//		//fmt.Println("transaction sort start...")
	//		TransactionSort(hashtable)
	//		instance.addHashTable(hashtable)
	//	}
	//	instances = append(instances, *instance)
	//}
	fmt.Print("block number:" + strconv.Itoa(totalNumber))
	startTime := time.Now()
	//peer.execInParalleling(execBlocks)
	peer.execInSequentialPlusImpl(blocks4SequentialPlus)
	fmt.Print("exec time:")
	fmt.Print(time.Since(startTime))
	startTime = time.Now()
	//peer.OperationAfterExecution(instances)
	peer.abortInSequentialPlus(blocks4SequentialPlus)
	fmt.Print(" abort time:")
	fmt.Print(time.Since(startTime))
	abortNumber := 0
	//for _, blocks := range execBlocks {
	//	for i := 0; i < len(blocks); i++ {
	//		for _, transaction := range blocks[i].txs {
	//			if transaction.abort {
	//				abortNumber += 1
	//			}
	//		}
	//	}
	//}
	for _, block := range blocks4SequentialPlus {
		for _, transaction := range block.txs {
			if transaction.abort {
				abortNumber += 1
			}
		}
	}
	fmt.Print(" abort rate:")
	fmt.Println(float64(abortNumber) / float64(totalNumber*config.BatchTxNum))
}
