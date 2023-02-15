package exec

import (
	"fmt"
	"math/rand"
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

type Peer struct {
	mu                sync.Mutex      // 并发
	id                int             // 节点id
	peersIds          []int           // 所有节点ID
	state             State           // 当前节点状态：普通节点(Normal)或者统计状态节点(Monitor)
	epochTimeout      time.Duration   // 每个epoch timeout
	epochTimeStamp    time.Time       // 最后一次执行epoch的时间
	blocks            []Block         // 当前节点所出的blocks
	NotExecBlockIndex int             // 块高最小的还未被执行的块高度
	epochState        map[int][]Block // 当前epoch所要执行的各个节点的块列表,key为节点id
	blockTimeout      time.Duration   // 出块时间
	blockTimeStamp    time.Time       // 最后一次出块的时间
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
		"	block height: " + strconv.Itoa(peer.getBlockHeight())
}
func (peer *Peer) log() string {
	return "(Log)" + peer.string()
}

// 获取块高
func (peer *Peer) getBlockHeight() int {
	return len(peer.blocks) - peer.NotExecBlockIndex
}

// 获取块高最小的还未被执行的块高度
func (peer *Peer) getNotExecBlockIndex() int {
	return peer.NotExecBlockIndex
}

// 根据节点id更新epochState内的块
func (peer *Peer) updateEpochState(id int, blocks []Block) {
	peer.epochState[id] = blocks
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
	newBlock := NewBlock(tx)
	peer.mu.Lock()
	peer.blocks = append(peer.blocks, *newBlock)
	peer.log()

}
func (peer *Peer) sendCheckBlockHeight(id int) int {
	fmt.Println("Monitor(id:" + strconv.Itoa(peer.id) + "） send message to check block height to peer(id:" + strconv.Itoa(id) + ")...")
	return peerList.peers[id].getBlockHeight()
}

// 启动节点
func (peer *Peer) start() {
	fmt.Println("Peer(id:" + strconv.Itoa(peer.id) + ") start...")
	//fmt.Println(peer)
	go func() {
		for {
			if peer.checkBlockTimeout() {
				peer.BlockOut()
				peer.getNewBlockTimeout()
			}
			if peer.state == Monitor {
				if peer.checkEpochTimeout() {
					var heightMap map[int]int
					for _, id := range peer.peersIds {
						var height = peer.sendCheckBlockHeight(id)
						heightMap[id] = height
					}
				}
			}
		}
	}()
}

// 停止节点
func (peer *Peer) stop() {
	peer.mu.Lock()
	peer.state = Dead
	fmt.Println("Peer(id:" + strconv.Itoa(peer.id) + ") Dead...")
	peer.mu.Unlock()
}
func init() {
	peerList.config = config
	var flag = false
	var state = Normal
	for i := 0; i < config.PeerNumber; i++ {
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
	var timestamp = time.Now()
	for _, peer := range peerList.peers {
		peer.blockTimeStamp = timestamp
		peer.epochTimeStamp = timestamp
		peer.peersIds = peerList.getPeerId()
		peer.start()
	}
}
