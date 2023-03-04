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
	records    map[int]*Record
	execNumber int
}

func newStatisticalResults(result map[int]*Record, execNumber int) *StatisticalResults {
	newResult := new(StatisticalResults)
	newResult.records = result
	newResult.execNumber = execNumber
	return newResult
}
func PeerStop() StatisticalResults {
	result := make([]map[int]*Record, 0)
	execNumber := make([]int, 0)
	for _, peer := range peerMap {
		peer.stop()
		result = append(result, peer.record)
		execNumber = append(execNumber, peer.execNumber.number)
	}

	return *newStatisticalResults(result[0], execNumber[0])
}
func PeerInit(runningType execType) {
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
		go tmp.start(runningType)
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
			fmt.Println(1 - float64(statisticalResults.execNumber)/(float64(totalExecBlockNumber)*float64(config.BatchTxNum)))
			break
		}
	}
}
