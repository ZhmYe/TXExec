package exec

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

// PeerMap 所有节点
var peerMap = make(map[int]Peer)

type State int

const (
	Normal State = iota
	Monitor
	Dead
)

type Record struct {
	id     int
	blocks []Block
	index  int // 还未被执行的区块index
}

func newRecord(id int) *Record {
	var record = new(Record)
	record.id = id
	record.blocks = make([]Block, 0)
	record.index = 0
	return record
}
func (record *Record) appendBlock(block Block) {
	record.blocks = append(record.blocks, block)
}

type Peer struct {
	mu             sync.Mutex    // 并发
	id             int           // 节点id
	peersIds       []int         // 所有节点ID
	state          State         // 当前节点状态：普通节点(Normal)或者统计状态节点(Monitor)
	epochTimeout   time.Duration // 每个epoch timeout
	epochTimeStamp time.Time     // 最后一次执行epoch的时间
	//blocks            []Block        // 当前节点所出的blocks
	//NotExecBlockIndex int            // 块高最小的还未被执行的块高度
	record         map[int]Record // 各个节点的出块记录, key为节点id
	blockTimeout   time.Duration  // 出块时间
	blockTimeStamp time.Time      // 最后一次出块的时间
	execNumber     int            //执行的ops数量
}

func newPeer(id int, state State, timestamp time.Time, peerId []int) *Peer {
	var peer = new(Peer)
	peer.id = id
	peer.state = state
	peer.epochTimeout = time.Duration(500) * time.Millisecond
	//peer.epochTimeStamp = time.Now()
	peer.getNewBlockTimeout()
	peer.blockTimeStamp = timestamp
	peer.epochTimeStamp = timestamp
	peer.peersIds = peerId
	peer.record = generateRecordMap(peerId)
	peer.execNumber = 0
	//record := make(map[int]Record, 0)
	//for _, index := range peerList.getPeerId() {
	//	record[index] = *newRecord(index)
	//}
	//peer.record = record
	//peer.NotExecBlockIndex = 0
	return peer
}
func (peer *Peer) string() string {
	var state string
	if peer.state == Monitor {
		state = "Monitor"
	} else {
		state = "Normal"
	}
	return "peer: {\n" +
		"	id: " + strconv.Itoa(peer.id) + "\n" +
		"	state: " + state + "\n" +
		"	block height: " + strconv.Itoa(peer.getBlockHeight()) + "\n}"
}

// 获取当前epoch中的{state: op->op}
func (peer *Peer) getHashTable(id int, bias int) map[string][]Op {
	//fmt.Println(peer.id)
	//fmt.Println(id)
	//fmt.Println(bias)
	//fmt.Println()
	hashtable := make(map[string][]Op)
	record := peer.record[id]
	for i := 0; i < bias; i++ {
		tmpTx := record.blocks[i+record.index].txs
		for _, tx := range tmpTx {
			for _, op := range tx.Ops {
				if hashtable[op.Key] == nil {
					hashtable[op.Key] = make([]Op, 0)
				}
				hashtable[op.Key] = append(hashtable[op.Key], op)
			}
		}
	}
	return hashtable
}

func (peer *Peer) execImpl(hashtable map[string][]Op) {
	// 暂时先写不同key串行
	keyTable := make([]string, 0)
	for key, _ := range hashtable {
		keyTable = append(keyTable, key)
	}
	var index = 0
	var jump = 4
	for {
		var wg sync.WaitGroup
		wg.Add(jump)
		for i := 0; i < jump; i++ {
			tmp := i
			go func(i int, wg *sync.WaitGroup) {
				defer wg.Done()
				if index+i >= len(keyTable) {
					return
				}
				for _, op := range hashtable[keyTable[index+i]] {
					if op.Type == OpRead {
						Read(op.Key)
					}
					if op.Type == OpWrite {
						Write(op.Key, op.Val)
					}
				}
			}(tmp, &wg)
		}
		wg.Wait()
		index += jump
		if index >= len(keyTable) {
			break
		}
	}
}
func (peer *Peer) addExecNumber(extra int) {
	peer.execNumber += extra
}
func (peer *Peer) exec(epoch map[int]int) {
	peer.mu.Lock()
	hashTables := make([]map[string][]Op, 0)
	for id, bias := range epoch {
		hashTables = append(hashTables, peer.getHashTable(id, bias))
	}
	solution := newSolution(hashTables)
	result := solution.getResult(IndexChoose)
	// 执行交易
	peer.execImpl(result)
	peer.addExecNumber(getOpsNumber(result))
	//fmt.Println("Peer" + strconv.Itoa(peer.id) + " exec ops:" + strconv.Itoa(getOpsNumber(result)))
	peer.log("exec ops:" + strconv.Itoa(getOpsNumber(result)))
	for _, id := range peer.peersIds {
		peer.UpdateIndexToRecord(id, epoch[id])
	}
	//peer.NotExecBlockIndex += epoch[peer.id]
	peer.mu.Unlock()

}
func (peer *Peer) RecordLog() string {
	result := ""
	for id, _ := range peer.record {
		tmp := strconv.Itoa(id) + ":" + strconv.Itoa(peer.record[id].index) + "/" + strconv.Itoa(len(peer.record[id].blocks)) + "\n"
		result += tmp
	}
	return result
}
func (peer *Peer) log(content string) {
	//var content = "(Log)" + peer.string()
	file, err := os.OpenFile("log/peer_"+strconv.Itoa(peer.id)+".log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.FileMode(0777))
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()
	_, err = file.WriteString(content + "\n")
	if err != nil {
		fmt.Println(err)
	}
}

// 获取块高
func (peer *Peer) getBlockHeight() int {
	return len(peer.record[peer.id].blocks) - peer.record[peer.id].index
}

// AppendBlockToRecord 根据节点id向record添加共识好的块
func (peer *Peer) AppendBlockToRecord(id int, block Block) {
	peer.mu.Lock()
	record4id := peer.record[id]
	record4id.appendBlock(block)
	peer.record[id] = record4id
	peer.mu.Unlock()
}
func (peer *Peer) UpdateIndexToRecord(id int, bias int) {
	//peer.mu.Lock()
	//fmt.Println(strconv.Itoa(peer.id) + "update" + " " + strconv.Itoa(id))
	record4id := peer.record[id]
	record4id.index += bias
	peer.record[id] = record4id
	//peer.mu.Unlock()
}

// 更新区块状态
//func (peer *Peer) updateBlockState(length int) {
//	peer.mu.Lock()
//	var startIndex = peer.NotExecBlockIndex
//	for i := 0; i < length; i++ {
//		peer.blocks[startIndex+length].UpdateState()
//	}
//	peer.mu.Unlock()
//}

// 更新出块timeout
func (peer *Peer) getNewBlockTimeout() {
	peer.mu.Lock()
	peer.blockTimeout = time.Duration(1000+rand.Intn(3000)) * time.Millisecond
	peer.blockTimeStamp = time.Now()
	peer.mu.Unlock()
}

// 是否需要出块
func (peer *Peer) checkBlockTimeout() bool {
	return time.Since(peer.blockTimeStamp) >= peer.blockTimeout
}
func (peer *Peer) updateEpochTimeStamp() {
	peer.epochTimeStamp = time.Now()
}

// 执行epoch
func (peer *Peer) checkEpochTimeout() bool {
	return time.Since(peer.epochTimeStamp) >= peer.epochTimeout
}
func (peer *Peer) BlockOut() {
	var tx = GenTxSet()
	//var tx = make([]*Tx, 0)
	newBlock := NewBlock(tx)
	peer.mu.Lock()
	//peer.blocks = append(peer.blocks, *newBlock)
	for _, eachPeer := range peerMap {
		//if eachPeer.id == peer.id {
		//	continue
		//}
		eachPeer.AppendBlockToRecord(peer.id, *newBlock)
	}
	//for _, id := range peer.peersIds {
	//	if id == peer.id {
	//		continue
	//	}
	//	peerList.peers[id].AppendBlockToRecord(peer.id, *newBlock)
	//}
	peer.mu.Unlock()
	peer.log(peer.string())

}
func (peer *Peer) sendCheckBlockHeight(id int) int {
	tmp := peerMap[id]
	return tmp.getBlockHeight()
}

// 启动节点
func (peer *Peer) start() {
	fmt.Println("Peer(id:" + strconv.Itoa(peer.id) + ") start...")
	peer.log("Peer(id:" + strconv.Itoa(peer.id) + ") start...")
	//fmt.Println(peer.string())
	peer.log(peer.string())
	go func(peer *Peer) {
		for {
			if peer.state == Dead {
				break
			}
			if peer.checkBlockTimeout() {
				peer.BlockOut()
				peer.getNewBlockTimeout()
			}
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}(peer)
	for {
		if peer.state == Dead {
			break
		}
		//fmt.Println(peer.id)
		if peer.state == Monitor {
			if peer.checkEpochTimeout() {
				peer.log(peer.RecordLog())
				//fmt.Println(peer.RecordLog())
				var heightMap map[int]int
				heightMap = make(map[int]int)
				//fmt.Println("Monitor(id:" + strconv.Itoa(peer.id) + "） send message to check block height to peers...")
				peer.log("Monitor(id:" + strconv.Itoa(peer.id) + "） send message to check block height to peers...")
				total := 0
				time.Sleep(time.Duration(100) * time.Millisecond) // 得到实时树高耗时
				for _, id := range peer.peersIds {
					var height = peer.sendCheckBlockHeight(id)
					//fmt.Println("height:" + strconv.Itoa(height))
					heightMap[id] = height
					total += height
				}
				if total > 10 {
					for id, height := range heightMap {
						tmp := int(math.Floor(float64(10) * float64(height) / float64(total)))
						heightMap[id] = tmp
					}
				}
				time.Sleep(time.Duration(100) * time.Millisecond) // 告诉其它节点epoch节点耗时
				var wg sync.WaitGroup
				wg.Add(len(peer.peersIds))
				// 根据heightMap得到各个节点剩余块高，然后计算epoch中的比例
				for _, eachPeer := range peerMap {
					tmp := eachPeer
					go func(tmp Peer, wg *sync.WaitGroup) {
						defer wg.Done()
						tmp.exec(heightMap)
					}(tmp, &wg)
					//eachPeer.exec(heightMap)
				}
				wg.Wait()

			}
		}
	}
}

// 停止节点
func (peer *Peer) stop() {
	peer.mu.Lock()
	peer.state = Dead
	peer.log("Peer(id:" + strconv.Itoa(peer.id) + ") Dead...")
	fmt.Println("Peer(id:" + strconv.Itoa(peer.id) + ") Dead...")
	peer.mu.Unlock()
}
func generateIds(number int) []int {
	result := make([]int, 0)
	for i := 0; i < number; i++ {
		result = append(result, i)
	}
	return result
}
func generateRecordMap(ids []int) map[int]Record {
	record := make(map[int]Record, 0)
	for _, id := range ids {
		record[id] = *newRecord(id)
	}
	return record
}
func PeerStop() map[int]Record {
	result := make([]map[int]Record, 0)
	for _, peer := range peerMap {
		peer.stop()
		result = append(result, peer.record)
	}
	return result[0]
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
		peerMap[id] = *peer
	}
	for _, peer := range peerMap {
		var tmp = peer
		go tmp.start()
	}
	timeStart := time.Now()
	for {
		totalExecBlockNumber := 0
		totalExecOpsNumber := 0
		if time.Since(timeStart) >= config.execTimeout {
			records := PeerStop()
			for _, record := range records {
				totalExecBlockNumber += record.index - 1
			}
			for _, peer := range peerMap {
				totalExecOpsNumber += peer.execNumber
			}
			fmt.Print("tps: ")
			fmt.Println(float64(totalExecBlockNumber) * float64(config.BatchTxNum) / float64(config.execTimeNumber))
			fmt.Print("abort rate:")
			fmt.Println(1 - float64(totalExecOpsNumber)/(float64(totalExecBlockNumber)*float64(config.BatchTxNum)*float64(config.OpsPerTx)))
			break
		}
	}
}
