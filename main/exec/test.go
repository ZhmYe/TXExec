package exec

import (
	"fmt"
	"math/rand"
	"time"
)

func generateIds(number int) []int {
	result := make([]int, 0)
	for i := 0; i < number; i++ {
		result = append(result, i)
	}
	return result
}
func generateRecordMap(ids []int) map[int]*Record {
	record := make(map[int]*Record, 0)
	for _, id := range ids {
		record[id] = newRecord(id)
	}
	return record
}

type StatisticalResults struct {
	records map[int]*Record
}

func (result *StatisticalResults) getExecNumber() int {
	execNumber := 0
	for _, record := range result.records {
		for i := 0; i < record.index; i++ {
			for _, tx := range record.blocks[i].txs {
				if !tx.abort {
					execNumber += 1
				}
			}
		}
	}
	return execNumber
}
func newStatisticalResults(result map[int]*Record) *StatisticalResults {
	newResult := new(StatisticalResults)
	newResult.records = result
	return newResult
}
func PeerStop() StatisticalResults {
	result := make([]map[int]*Record, 0)
	for _, peer := range peerMap {
		peer.stop()
		result = append(result, peer.record)
	}

	return *newStatisticalResults(result[0])
}
func PeerInit() {
	//peerList.config = config
	peerId := generateIds(config.PeerNumber)
	var timestamp = time.Now()
	var flag = false
	for i, id := range peerId {
		var state = Normal
		if !flag && rand.Intn(config.PeerNumber) == 1 {
			flag = true
			state = Monitor
		}
		if !flag && i == config.PeerNumber-1 {
			state = Monitor
			flag = true
		}
		var peer = newPeer(id, state, timestamp, peerId)
		//peerList.peers = append(peerList.peers, *peer)
		peerMap[id] = peer
	}
	for _, peer := range peerMap {
		var tmp = peer
		go tmp.start()
	}
	timeStart := time.Now()
	for {
		totalExecBlockNumber := 0
		if time.Since(timeStart) >= config.execTimeout {
			statisticalResults := PeerStop()
			for _, record := range statisticalResults.records {
				totalExecBlockNumber += record.index
			}
			//fmt.Println(totalExecBlockNumber)
			//fmt.Println(statisticalResults.execNumber)
			fmt.Print("tps: ")
			fmt.Println(float64(totalExecBlockNumber) * float64(config.BatchTxNum) / float64(config.execTimeNumber))
			fmt.Print("abort rate:")
			fmt.Println(1 - float64(statisticalResults.getExecNumber())/(float64(totalExecBlockNumber)*float64(config.BatchTxNum)))
			break
		}
	}
}
