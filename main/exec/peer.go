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
	tmpTx := record.blocks[bias+record.index].txs
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
	return hashtable
}
func (peer *Peer) exec(epoch map[int]int) {
	peer.mu.Lock()
	//fmt.Println("exec start...")
	if len(epoch) != 0 {
		instances := make([]Instance, 0)
		execBlocks := make(map[int][]Block, 0)
		for id, bias := range epoch {
			record := peer.record[id]
			execBlocks[id] = record.blocks[record.index : record.index+bias]
			instance := newInstance(id)
			for i := 0; i < bias; i++ {
				hashtable := peer.getHashTable(id, i)
				//fmt.Println("transaction sort start...")
				TransactionSort(hashtable)
				instance.addHashTable(hashtable)
			}
			instances = append(instances, *instance)
		}
		peer.execInParalleling(execBlocks)
		peer.OperationAfterExecution(instances)
		for _, id := range peer.peersIds {
			record4id := peer.record[id]
			record4id.index += epoch[id]
			peer.record[id] = record4id
		}
	}
	peer.mu.Unlock()

}

// execInParalleling 并行执行不同Instance中的区块
func (peer *Peer) execInParalleling(ExecBlocks map[int][]Block) {
	var wg sync.WaitGroup
	wg.Add(len(ExecBlocks))
	//buffer := make(map[string]int, 0)
	//for _, address := range smallbank.savings {
	//	tmpResult, _ := strconv.Atoi(smallbank.Read(address))
	//	buffer[address] = tmpResult
	//}
	//for _, address := range smallbank.checkings {
	//	tmpResult, _ := strconv.Atoi(smallbank.Read(address))
	//	buffer[address] = tmpResult
	//}
	for _, blocks := range ExecBlocks {
		tmpBlocks := blocks
		//tmpBuffer := buffer
		go func(blocks []Block, wg *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < len(blocks); i++ {
				var buffer sync.Map
				var wg4tx sync.WaitGroup
				wg4tx.Add(len(blocks[i].txs))
				for _, transaction := range blocks[i].txs {
					tmpTx := transaction
					go func(tx *Tx, wg4tx *sync.WaitGroup) {
						defer wg4tx.Done()
						switch tx.txType {
						case transactSavings:
							readOp := tx.Ops[0]
							writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
							// 第一个块读取数据库内容
							if i == 0 {
								readResult, _ := strconv.Atoi(smallbank.Read(readOp.Key))
								WriteResult := readResult + writeValue
								tx.Ops[1].Val = strconv.Itoa(WriteResult) // 这里用于后续更新数据库execLastWrite
								buffer.Store(readOp.Key, WriteResult)
								//buffer[readOp.Key] = WriteResult
							} else {
								// 后续块读取前一个块的结果，如果没有buffer读取数据库
								readResult, exist := buffer.Load(readOp.Key)
								//readResult, exist := buffer[readOp.Key]
								if !exist {
									readResult, _ = strconv.Atoi(smallbank.Read(readOp.Key))
								}
								WriteResult := readResult.(int) + writeValue
								tx.Ops[1].Val = strconv.Itoa(WriteResult)
								buffer.Store(readOp.Key, WriteResult)
								//buffer[readOp.Key] = WriteResult
							}
						case depositChecking:
							readOp := tx.Ops[0]
							writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
							if i == 0 {
								readResult, _ := strconv.Atoi(smallbank.Read(readOp.Key))
								WriteResult := readResult + writeValue
								tx.Ops[1].Val = strconv.Itoa(WriteResult) // 这里用于后续更新数据库execLastWrite
								buffer.Store(readOp.Key, WriteResult)
								//buffer[readOp.Key] = WriteResult
							} else {
								readResult, exist := buffer.Load(readOp.Key)
								if !exist {
									readResult, _ = strconv.Atoi(smallbank.Read(readOp.Key))
								}
								WriteResult := readResult.(int) + writeValue
								tx.Ops[1].Val = strconv.Itoa(WriteResult)
								buffer.Store(readOp.Key, WriteResult)
								//buffer[readOp.Key] = WriteResult
							}
						case sendPayment:
							readOpA := tx.Ops[0]
							readOpB := tx.Ops[1]
							writeValueA, _ := strconv.Atoi(tx.Ops[2].Val)
							writeValueB, _ := strconv.Atoi(tx.Ops[3].Val)
							if i == 0 {
								readResultA, _ := strconv.Atoi(smallbank.Read(readOpA.Key))
								readResultB, _ := strconv.Atoi(smallbank.Read(readOpB.Key))
								WriteResultA := readResultA + writeValueA
								WriteResultB := readResultB + writeValueB
								tx.Ops[2].Val = strconv.Itoa(WriteResultA)
								buffer.Store(readOpA.Key, writeValueA)
								//buffer[readOpA.Key] = writeValueA
								tx.Ops[3].Val = strconv.Itoa(WriteResultB)
								buffer.Store(readOpB.Key, writeValueB)
								//buffer[readOpB.Key] = writeValueB
							} else {
								readResultA, exist := buffer.Load(readOpA.Key)
								if !exist {
									readResultA, _ = strconv.Atoi(smallbank.Read(readOpA.Key))
								}
								readResultB, exist := buffer.Load(readOpB.Key)
								if !exist {
									readResultB, _ = strconv.Atoi(smallbank.Read(readOpB.Key))
								}
								WriteResultA := readResultA.(int) + writeValueA
								WriteResultB := readResultB.(int) + writeValueB
								tx.Ops[2].Val = strconv.Itoa(WriteResultA)
								buffer.Store(readOpA.Key, writeValueA)
								//buffer[readOpA.Key] = writeValueA
								tx.Ops[3].Val = strconv.Itoa(WriteResultB)
								buffer.Store(readOpB.Key, writeValueB)
								//buffer[readOpB.Key] = writeValueB
							}
						case writeCheck:
							readOp := tx.Ops[0]
							writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
							if i == 0 {
								readResult, _ := strconv.Atoi(smallbank.Read(readOp.Key))
								WriteResult := readResult + writeValue
								tx.Ops[1].Val = strconv.Itoa(WriteResult) // 这里用于后续更新数据库execLastWrite
								buffer.Store(readOp.Key, WriteResult)
								//buffer[readOp.Key] = WriteResult
							} else {
								readResult, exist := buffer.Load(readOp.Key)
								if !exist {
									readResult, _ = strconv.Atoi(smallbank.Read(readOp.Key))
								}
								WriteResult := readResult.(int) + writeValue
								tx.Ops[1].Val = strconv.Itoa(WriteResult)
								buffer.Store(readOp.Key, WriteResult)
							}
						case query:
							readOpSaving := tx.Ops[0]
							readOpChecking := tx.Ops[1]
							if i == 0 {
								smallbank.Read(readOpSaving.Key)
								smallbank.Read(readOpChecking.Key)
							} else {
								_, exist := buffer.Load(readOpSaving.Key)
								if !exist {
									smallbank.Read(readOpSaving.Key)
								}
								_, exist = buffer.Load(readOpChecking.Key)
								if !exist {
									smallbank.Read(readOpChecking.Key)
								}
							}
						case amalgamate:
							readOpSaving := tx.Ops[0]
							readOpChecking := tx.Ops[1]
							if i == 0 {
								readResultSaving, _ := strconv.Atoi(smallbank.Read(readOpSaving.Key))
								WriteResultSaving := 0
								tx.Ops[2].Val = strconv.Itoa(WriteResultSaving)
								buffer.Store(readOpSaving.Key, 0)
								readResultChecking, _ := strconv.Atoi(smallbank.Read(readOpChecking.Key))
								writeResultChecking := readResultSaving + readResultChecking
								tx.Ops[3].Val = strconv.Itoa(writeResultChecking)
								buffer.Store(readOpChecking.Key, writeResultChecking)
							} else {
								readResultSaving, exist := buffer.Load(readOpSaving.Key)
								if !exist {
									readResultSaving, _ = strconv.Atoi(smallbank.Read(readOpSaving.Key))
								}
								readResultChecking, exist := buffer.Load(readOpChecking.Key)
								if !exist {
									readResultChecking, _ = strconv.Atoi(smallbank.Read(readOpChecking.Key))
								}
								writeResultSaving := 0
								tx.Ops[2].Val = strconv.Itoa(writeResultSaving)
								buffer.Store(readOpSaving.Key, 0)
								writeResultChecking := readResultSaving.(int) + readResultChecking.(int)
								tx.Ops[3].Val = strconv.Itoa(writeResultChecking)
								buffer.Store(readOpChecking.Key, writeResultChecking)

							}
						}
					}(tmpTx, &wg4tx)
				}
				wg4tx.Wait()
			}
		}(tmpBlocks, &wg)
	}
	wg.Wait()
}

// OperationAfterExecution 每个Instance的hashtable(多个子块串联r-w-r-w)
// 计算每个Instance在每个Address上的读集的级联度，并联合所有的Instance
// 结构：
// map[address] -> OrderInstance{variance, instances, ...}
// 根据上述map, 计算每个map的方差， 按序构建Graph
func (peer *Peer) OperationAfterExecution(instances []Instance) {
	var wg4computeCascade sync.WaitGroup
	wg4computeCascade.Add(len(instances))
	// 并行计算所有Instance级联度
	//fmt.Println("cascade compute start...")
	for _, instance := range instances {
		tmpInstance := instance
		go func(instance Instance, wg4computeCascade *sync.WaitGroup) {
			defer wg4computeCascade.Done()
			instance.computeCascade() // 计算每个instance的级联度
		}(tmpInstance, &wg4computeCascade)
	}
	wg4computeCascade.Wait()
	// 获取所有address所对应的Instances,用于排序
	OrderInstanceMap := make(map[string]OrderInstance, 0)
	instanceDict := make(map[int]int, 0) // 对应有向图坐标
	tmpIndex := 0
	for _, instance := range instances {
		instanceDict[instance.peerId] = tmpIndex
		tmpIndex++
		for address, _ := range instance.cascade {
			_, ok := OrderInstanceMap[address]
			if !ok {
				OrderInstanceMap[address] = *newOrderInstance(address)
			}
			tmpOrderInstance := OrderInstanceMap[address]
			tmpOrderInstance.appendInstance(instance)
			OrderInstanceMap[address] = tmpOrderInstance
		}
	}
	DAG := make([][]int, tmpIndex) // 有向图邻接矩阵
	for i := range DAG {
		DAG[i] = make([]int, tmpIndex)
		for j := range DAG[i] {
			DAG[i][j] = 0
		}
	}
	// 计算方差并对address进行排序
	List4AddressOrder := make([]string, 0)
	for address, orderInstance := range OrderInstanceMap {
		orderInstance.computeVariance()
		List4AddressOrder = append(List4AddressOrder, address)
	}
	// 冒泡排序, List4Address里的顺序就是最后Address的顺序
	AddressSortFlag := true
	for i := 0; i < len(List4AddressOrder)-1; i++ {
		AddressSortFlag = true
		// 方差倒排，在方差一样的基础上看谁的instance多
		for j := 0; j < len(List4AddressOrder)-i-1; j++ {
			if OrderInstanceMap[List4AddressOrder[j]].variance < OrderInstanceMap[List4AddressOrder[j+1]].variance {
				List4AddressOrder[j], List4AddressOrder[j+1] = List4AddressOrder[j+1], List4AddressOrder[j]
				AddressSortFlag = false
			} else if OrderInstanceMap[List4AddressOrder[j]].variance == OrderInstanceMap[List4AddressOrder[j+1]].variance {
				if len(OrderInstanceMap[List4AddressOrder[j]].instances) < len(OrderInstanceMap[List4AddressOrder[i]].instances) {
					List4AddressOrder[j], List4AddressOrder[j+1] = List4AddressOrder[j+1], List4AddressOrder[j]
					AddressSortFlag = false
				}
			}
		}
		if AddressSortFlag {
			break
		}
	}
	// 根据排好序的address，得到其对应的instances的顺序，放入有向图中
	for _, address := range List4AddressOrder {
		tmpOrderInstance := OrderInstanceMap[address]
		result := tmpOrderInstance.getOrder()
		// 按序检测该结果是否与之前有冲突，如果有，将该元素删除
		validResult := make([]int, 0)
		for i, element := range result {
			valid := true
			for j := i + 1; j < len(result); j++ {
				// 如果与之前的顺序冲突
				if DAG[instanceDict[result[j]]][instanceDict[result[i]]] == 1 {
					valid = false
					break
				}
			}
			if valid {
				validResult = append(validResult, instanceDict[element])
			}
		}
		// 将新加入的顺序更新到DAG中
		for i := 0; i < len(validResult)-1; i++ {
			DAG[validResult[i]][validResult[i+1]] = 1
		}
	}
	//var execWg sync.WaitGroup
	//execWg.Add(len(OrderInstanceMap))
	topologicalOrder := TopologicalOrder(DAG)
	for _, orderInstance := range OrderInstanceMap {
		tmpInstance := orderInstance
		//fmt.Println("start go func....")
		//go func(tmpInstance OrderInstance, execWg *sync.WaitGroup) {
		//	defer execWg.Done()
		tmpInstance.OrderByDAG(topologicalOrder, instanceDict)
		tmpInstance.execLastWrite()
		//fmt.Println("execLastWrite success...")
		//}(tmpInstance, &execWg)
	}
	//execWg.Wait()
}
func (peer *Peer) addExecNumber(extra int) {
	tmp := peer.execNumber
	tmp.number += extra
	peer.execNumber = tmp
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
	peer.blockTimeout = time.Duration(200*peer.id+rand.Intn(400)) * time.Millisecond
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
	var tx = smallbank.GenTxSet(config.BatchTxNum)
	peer.log("generate tx:" + strconv.Itoa(len(tx)))
	newBlock := NewBlock(tx)
	for _, eachPeer := range peerMap {
		eachPeer.AppendBlockToRecord(peer.id, *newBlock)
	}
	peer.log(peer.string())

}
func (peer *Peer) sendCheckBlockHeight(id int) int {
	tmp := peerMap[id]
	return tmp.getBlockHeight()
}

// 启动节点 Mine模式
func (peer *Peer) run() {
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
					if height == 0 {
						continue
					}
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
						tmp.exec(heightMap)
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
			switch tx.txType {
			case transactSavings:
				readOp := tx.Ops[0]
				writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
				readResult, _ := strconv.Atoi(smallbank.Read(readOp.Key))
				WriteResult := readResult + writeValue
				tx.Ops[1].Val = strconv.Itoa(WriteResult)
				smallbank.Update(readOp.Key, strconv.Itoa(WriteResult))
			case depositChecking:
				readOp := tx.Ops[0]
				writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
				readResult, _ := strconv.Atoi(smallbank.Read(readOp.Key))
				WriteResult := readResult + writeValue
				tx.Ops[1].Val = strconv.Itoa(WriteResult)
				smallbank.Update(readOp.Key, strconv.Itoa(WriteResult))
			case sendPayment:
				readOpA := tx.Ops[0]
				readOpB := tx.Ops[1]
				writeValueA, _ := strconv.Atoi(tx.Ops[2].Val)
				writeValueB, _ := strconv.Atoi(tx.Ops[3].Val)
				readResultA, _ := strconv.Atoi(smallbank.Read(readOpA.Key))
				readResultB, _ := strconv.Atoi(smallbank.Read(readOpB.Key))
				WriteResultA := readResultA + writeValueA
				WriteResultB := readResultB + writeValueB
				tx.Ops[2].Val = strconv.Itoa(WriteResultA)
				smallbank.Update(readOpA.Key, strconv.Itoa(writeValueA))
				tx.Ops[3].Val = strconv.Itoa(WriteResultB)
				smallbank.Update(readOpB.Key, strconv.Itoa(writeValueB))
			case writeCheck:
				readOp := tx.Ops[0]
				writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
				readResult, _ := strconv.Atoi(smallbank.Read(readOp.Key))
				WriteResult := readResult + writeValue
				tx.Ops[1].Val = strconv.Itoa(WriteResult)
				smallbank.Update(readOp.Key, strconv.Itoa(WriteResult))
			case query:
				readOpSaving := tx.Ops[0]
				readOpChecking := tx.Ops[1]
				smallbank.Read(readOpSaving.Key)
				smallbank.Read(readOpChecking.Key)
			case amalgamate:
				readOpSaving := tx.Ops[0]
				readOpChecking := tx.Ops[1]
				readResultSaving, _ := strconv.Atoi(smallbank.Read(readOpSaving.Key))
				readResultChecking, _ := strconv.Atoi(smallbank.Read(readOpChecking.Key))
				writeResultSaving := 0
				tx.Ops[2].Val = strconv.Itoa(writeResultSaving)
				smallbank.Update(readOpSaving.Key, strconv.Itoa(0))
				writeResultChecking := readResultSaving + readResultChecking
				tx.Ops[3].Val = strconv.Itoa(writeResultChecking)
				smallbank.Update(readOpChecking.Key, strconv.Itoa(writeResultChecking))
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

// 启动节点 Sequential模式
func (peer *Peer) runInSequential() {
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
func (peer *Peer) start() {
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
	switch config.RunType {
	case Mine:
		peer.run()
	case Sequential:
		peer.runInSequential()
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
