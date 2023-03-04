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
var peerMap = make(map[int]*Peer)

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

type Unit struct {
	op     Op     // 实际执行的操作
	tx     *Tx    // 交易标识
	txHash string //交易哈希
}

func newUnit(op Op, tx *Tx, txHash string) *Unit {
	unit := new(Unit)
	unit.op = op
	unit.tx = tx
	unit.txHash = txHash
	return unit
}

type OpsNumber struct {
	number int
	id     int
}

func newOpsNumber(number int, id int) *OpsNumber {
	opsNumber := new(OpsNumber)
	opsNumber.number = number
	opsNumber.id = id
	return opsNumber
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
	record         map[int]*Record // 各个节点的出块记录, key为节点id
	blockTimeout   time.Duration   // 出块时间
	blockTimeStamp time.Time       // 最后一次出块的时间
	execNumber     OpsNumber       //执行的ops数量
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
	peer.execNumber = *newOpsNumber(0, peer.id)
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
func (peer *Peer) getHashTable(id int, bias int) map[string]StateSet {
	hashtable := make(map[string]StateSet)
	record := peer.record[id]
	for i := 0; i < bias; i++ {
		tmpTx := record.blocks[i+record.index].txs
		for txIndex, tx := range tmpTx {
			if tx.abort {
				continue
			}
			for _, op := range tx.Ops {
				_, ok := hashtable[op.Key]
				if !ok {
					hashtable[op.Key] = *newStateSet()
				}
				txHash := strconv.Itoa(rand.Intn(config.PeerNumber)) + "_" + strconv.Itoa(rand.Intn(10)) + "_" + strconv.Itoa(txIndex)
				unit := newUnit(op, tx, txHash)
				stateSet := hashtable[op.Key]
				//fmt.Println(len(stateSet.ReadSet), len(stateSet.WriteSet))
				if unit.op.Type == OpRead {
					stateSet.appendToReadSet(*unit)
				} else {
					stateSet.appendToWriteSet(*unit)
				}
				hashtable[op.Key] = stateSet
			}
		}
	}
	return hashtable
}

func (peer *Peer) execParallelingImpl(epoch map[int]int) {
	//keyTable := make([]string, 0)
	//for key, _ := range hashtable {
	//	keyTable = append(keyTable, key)
	//}
	//var index = 0
	//var jump = 4
	var wg sync.WaitGroup
	wg.Add(config.PeerNumber)
	//channel := make(chan int, config.PeerNumber)
	for id, bias := range epoch {
		tmpId := id
		tmpBias := bias
		hashtable := peer.getHashTable(id, bias)
		TransactionSort(hashtable)
		go func(id int, bias int, wg *sync.WaitGroup) {
			defer wg.Done()
			record := peer.record[id]
			allBlock := record.blocks[record.index : record.index+bias]
			orderedTxs := getAllOrderTxs(allBlock)
			for _, each := range orderedTxs {
				var wg2 sync.WaitGroup
				wg2.Add(len(each.txs))
				for _, tx := range each.txs {
					tmpTx := tx
					go func(tmpTx Tx, wg2 *sync.WaitGroup) {
						defer wg2.Done()
						for _, op := range tmpTx.Ops {
							if op.Type == OpRead {
								Read(op.Key)
							} else {
								Write(op.Key, op.Val)
							}
						}
					}(tmpTx, &wg2)
				}
				wg2.Wait()
			}
		}(tmpId, tmpBias, &wg)
		//go func(id int, bias int, wg *sync.WaitGroup) {
		//	defer wg.Done()
		//	//txNumber := 0
		//	record := peer.record[id]
		//	for i := 0; i < bias; i++ {
		//		txs := record.blocks[i+record.index].txs
		//		for _, tx := range txs {
		//			if !tx.abort {
		//				//fmt.Println(tx.sequence)
		//				//txNumber += 1
		//				for _, op := range tx.Ops {
		//					if op.Type == OpRead {
		//						Read(op.Key)
		//					} else {
		//						Write(op.Key, op.Val)
		//					}
		//				}
		//			}
		//		}
		//	}
		//	//channel <- txNumber
		//}(tmpId, tmpBias, &wg)
	}
	wg.Wait()
	//total := 0
	//for i := 0; i < config.PeerNumber; i++ {
	//	total += <-channel
	//}
	//close(channel)
	//return total
	//for {
	//	var wg sync.WaitGroup
	//	wg.Add(jump)
	//	for i := 0; i < jump; i++ {
	//		tmp := i
	//		go func(i int, wg *sync.WaitGroup) {
	//			defer wg.Done()
	//			if index+i >= len(keyTable) {
	//				return
	//			}
	//			for _, unit := range hashtable[keyTable[index+i]].ReadSet {
	//				op := unit.op
	//				Read(op.Key)
	//			}
	//			for _, unit := range hashtable[keyTable[index+i]].WriteSet {
	//				op := unit.op
	//				Write(op.Key, op.Val)
	//			}
	//		}(tmp, &wg)
	//	}
	//	wg.Wait()
	//	index += jump
	//	if index >= len(keyTable) {
	//		break
	//	}
	//}
}
func (peer *Peer) addExecNumber(extra int) {
	tmp := peer.execNumber
	tmp.number += extra
	peer.execNumber = tmp
}

func (peer *Peer) execParalleling(epoch map[int]int) {
	peer.mu.Lock()
	hashTables := make([]map[string]StateSet, 0)
	for id, bias := range epoch {
		if bias == 0 {
			continue
		}
		hashTables = append(hashTables, peer.getHashTable(id, bias))
	}
	if len(hashTables) != 0 {
		solution := newSolution(hashTables)
		solution.getResult()
		// 执行交易
		peer.execParallelingImpl(epoch)
		//peer.addExecNumber(getOpsNumber(result))
		//peer.log("exec txs:" + strconv.Itoa(total))
		for _, id := range peer.peersIds {
			record4id := peer.record[id]
			record4id.index += epoch[id]
			peer.record[id] = record4id
		}
		//peer.execNumber.number += total
		//peer.NotExecBlockIndex += epoch[peer.id]
	}
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
	record4id := peer.record[id]
	record4id.index += bias
	peer.record[id] = record4id
	//peer.mu.Unlock()
}

// 更新出块timeout
func (peer *Peer) getNewBlockTimeout() {
	peer.mu.Lock()
	peer.blockTimeout = time.Duration(1000*(peer.id+1)+rand.Intn(1000)) * time.Millisecond
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
	//peer.mu.Lock()
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
	//peer.mu.Unlock()
	peer.log(peer.string())

}
func (peer *Peer) sendCheckBlockHeight(id int) int {
	tmp := peerMap[id]
	return tmp.getBlockHeight()
}

type execType int

const (
	Waiting execType = iota
	Paralleling
)

// 启动节点
func (peer *Peer) runParalleling() {
	for {
		if peer.state == Dead {
			break
		}
		if peer.state == Monitor {
			if peer.checkEpochTimeout() {
				peer.log(peer.RecordLog())
				var heightMap map[int]int
				heightMap = make(map[int]int)
				peer.log("Monitor(id:" + strconv.Itoa(peer.id) + "） send message to check block height to peers...")
				total := 0
				time.Sleep(time.Duration(100) * time.Millisecond) // 得到实时树高耗时
				for _, id := range peer.peersIds {
					var height = peer.sendCheckBlockHeight(id)
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
					go func(tmp *Peer, wg *sync.WaitGroup) {
						defer wg.Done()
						tmp.execParalleling(heightMap)
					}(tmp, &wg)
					//eachPeer.exec(heightMap)
				}
				wg.Wait()

			}
		}
	}
}
func (peer *Peer) checkComplete() bool {
	peer.mu.Lock()
	flag := true
	for _, id := range peer.peersIds {
		height := len(peer.record[id].blocks) - peer.record[id].index
		if height == 0 {
			flag = false
		}
	}
	peer.mu.Unlock()
	return flag
}
func (peer *Peer) execWaitingImpl(blocks []Block) {
	for _, block := range blocks {
		for _, tx := range block.txs {
			for _, op := range tx.Ops {
				if op.Type == OpRead {
					Read(op.Key)
				}
				if op.Type == OpWrite {
					Write(op.Key, op.Val)
				}
			}
		}
	}
}
func (peer *Peer) execWaiting() {
	peer.mu.Lock()
	blocks := make([]Block, 0)
	for _, record := range peer.record {
		block := record.blocks[record.index]
		blocks = append(blocks, block)
	}
	// 执行交易
	peer.execWaitingImpl(blocks)
	peer.log("exec ops:" + strconv.Itoa(len(peer.peersIds)*config.BatchTxNum*config.OpsPerTx))
	for _, id := range peer.peersIds {
		record4id := peer.record[id]
		record4id.index += 1
		peer.record[id] = record4id
	}
	peer.execNumber.number += len(peer.peersIds) * config.BatchTxNum
	//peer.NotExecBlockIndex += epoch[peer.id]
	peer.mu.Unlock()
}
func (peer *Peer) runWaiting() {
	for {
		if peer.state == Dead {
			break
		}
		if peer.checkComplete() {
			peer.log(peer.RecordLog())
			//fmt.Println(peer.RecordLog())
			peer.execWaiting()
		}
	}
}
func (peer *Peer) start(params execType) {
	fmt.Println("Peer(id:" + strconv.Itoa(peer.id) + ") start...")
	peer.log("Peer(id:" + strconv.Itoa(peer.id) + ") start...")
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
	if params == Paralleling {
		peer.runParalleling()
	} else if params == Waiting {
		peer.runWaiting()
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
