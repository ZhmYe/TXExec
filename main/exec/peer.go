package exec

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

// PeerList 所有节点
type PeerList struct {
	config Config
	peers  []Peer
}

// 返回节点个数
func (peerList *PeerList) getPeerNumber() int {
	return len(peerList.peers)
}

// 返回所有节点id
func (peerList *PeerList) getPeerId() []int {
	var result = make([]int, 0, peerList.config.PeerNumber)
	for _, peer := range peerList.peers {
		result = append(result, peer.id)
	}
	return result
}

var peerList = new(PeerList)

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
	mu                sync.Mutex     // 并发
	id                int            // 节点id
	peersIds          []int          // 所有节点ID
	state             State          // 当前节点状态：普通节点(Normal)或者统计状态节点(Monitor)
	epochTimeout      time.Duration  // 每个epoch timeout
	epochTimeStamp    time.Time      // 最后一次执行epoch的时间
	blocks            []Block        // 当前节点所出的blocks
	NotExecBlockIndex int            // 块高最小的还未被执行的块高度
	record            map[int]Record // 各个节点的出块记录, key为节点id
	blockTimeout      time.Duration  // 出块时间
	blockTimeStamp    time.Time      // 最后一次出块的时间
}

func newPeer(id int, state State) *Peer {
	var peer = new(Peer)
	peer.id = id
	peer.state = state
	peer.epochTimeout = time.Duration(400) * time.Millisecond
	//peer.epochTimeStamp = time.Now()
	peer.getNewBlockTimeout()
	peer.NotExecBlockIndex = 0
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
	//NotExecBlockIndex
	// op : {type, key, value}
	hashtable := make(map[string][]Op)
	record := peer.record[id]
	for i := 0; i < bias; i++ {
		tmpTx := record.blocks[i].txs
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
func (peer *Peer) exec(epoch map[int]int) {
	peer.mu.Lock()
	hashTables := make([]map[string][]Op, 0)
	for id, bias := range epoch {
		hashTables = append(hashTables, peer.getHashTable(id, bias))
	}
	solution := newSolution(hashTables)
	result := solution.getResult(IndexChoose)
	//fmt.Println("Tx execution by result..")
	peer.log("exec ops:" + strconv.Itoa(getOpsNumber(result)))
	for _, id := range peer.peersIds {
		peerList.peers[id].UpdateIndexToRecord(id, epoch[id])
	}
	peer.NotExecBlockIndex += epoch[peer.id]
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
	return len(peer.blocks) - peer.NotExecBlockIndex
}

// 获取块高最小的还未被执行的块高度
func (peer *Peer) getNotExecBlockIndex() int {
	return peer.NotExecBlockIndex
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
	peer.mu.Lock()
	record4id := peer.record[id]
	record4id.index += bias
	peer.record[id] = record4id
	peer.mu.Unlock()
}

// 更新区块状态
func (peer *Peer) updateBlockState(length int) {
	peer.mu.Lock()
	var startIndex = peer.NotExecBlockIndex
	for i := 0; i < length; i++ {
		peer.blocks[startIndex+length].UpdateState()
	}
	peer.mu.Unlock()
}

// 更新出块timeout
func (peer *Peer) getNewBlockTimeout() {
	peer.mu.Lock()
	peer.blockTimeout = time.Duration(100+rand.Intn(300)) * time.Millisecond
	peer.blockTimeStamp = time.Now()
	peer.mu.Unlock()
}

// 是否需要出块
func (peer *Peer) checkBlockTimeout() bool {
	//fmt.Println(time.Since(peer.blockTimeStamp))
	//fmt.Println(peer.blockTimeout)
	return time.Since(peer.blockTimeStamp) >= peer.blockTimeout
}
func (peer *Peer) updateEpochTimeStamp() {
	peer.epochTimeStamp = time.Now()
}

// 是否需要出块
func (peer *Peer) checkEpochTimeout() bool {
	return time.Since(peer.epochTimeStamp) >= peer.epochTimeout
}
func (peer *Peer) BlockOut() {
	var tx = GenTxSet()
	//var tx = make([]*Tx, 0)
	newBlock := NewBlock(tx)
	peer.mu.Lock()
	peer.blocks = append(peer.blocks, *newBlock)
	for _, id := range peer.peersIds {
		if id == peer.id {
			continue
		}
		peerList.peers[id].AppendBlockToRecord(id, *newBlock)
	}
	peer.mu.Unlock()
	//fmt.Println(peer.string())
	peer.log(peer.string())

}
func (peer *Peer) sendCheckBlockHeight(id int) int {
	return peerList.peers[id].getBlockHeight()
}

// 启动节点
func (peer *Peer) start() {
	//_, err := os.Create("log/peer_" + strconv.Itoa(peer.id) + ".log")
	//if err != nil {
	//fmt.Println(err)
	//}
	fmt.Println("Peer(id:" + strconv.Itoa(peer.id) + ") start...")
	peer.log("Peer(id:" + strconv.Itoa(peer.id) + ") start...")
	fmt.Println(peer.string())
	peer.log(peer.string())
	for {
		//fmt.Println(peer.id)
		if peer.checkBlockTimeout() {
			peer.BlockOut()
			peer.getNewBlockTimeout()
		}
		if peer.state == Monitor {
			if peer.checkEpochTimeout() {
				peer.log(peer.RecordLog())
				fmt.Println(peer.RecordLog())
				var heightMap map[int]int
				heightMap = make(map[int]int)
				peer.log("Monitor(id:" + strconv.Itoa(peer.id) + "） send message to check block height to peers...")
				for _, id := range peer.peersIds {
					var height = peer.sendCheckBlockHeight(id)
					heightMap[id] = height
				}
				// 根据heightMap得到各个节点剩余块高，然后计算epoch中的比例
				for _, id := range peerList.getPeerId() {
					peerList.peers[id].exec(heightMap)
				}

			}
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
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
func PeerInit() {
	peerList.config = config
	record := make(map[int]Record, 0)
	for _, id := range peerList.getPeerId() {
		record[id] = *newRecord(id)
	}
	var flag = false
	for i := 0; i < config.PeerNumber; i++ {
		var state = Normal
		if !flag && rand.Intn(config.PeerNumber) == 1 {
			flag = true
			state = Monitor
		}
		if !flag && i == config.PeerNumber-1 {
			state = Monitor
			flag = true
		}
		var peer = newPeer(i, state)
		peerList.peers = append(peerList.peers, *peer)
	}
	//fmt.Println(peerList.getPeerId())
	var timestamp = time.Now()
	for _, peer := range peerList.peers {
		peer.blockTimeStamp = timestamp
		peer.epochTimeStamp = timestamp
		peer.peersIds = peerList.getPeerId()
		peer.record = record
		var tmp = peer
		go tmp.start()
	}
}
