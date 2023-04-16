package exec

import (
	"crypto"
	"crypto/rsa"
	"fmt"
	"math"
	"math/rand"
	//"os"
	"strconv"
	"sync"
	"time"
)

// PeerMap 所有节点
//var peerMap = make(map[int]*Peer)

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
	smallBank      *Smallbank      // 每个节点的smallbank
}

func newPeer(id int, state State, timestamp time.Time, peerId []int, saving []string, savingAmount []int, checking []string, checkingAmount []int, publicKey *rsa.PublicKey, hashed [32]byte, signature []byte) *Peer {
	var peer = new(Peer)
	peer.id = id
	peer.state = state
	peer.epochTimeout = time.Duration(100) * time.Millisecond
	//peer.epochTimeStamp = time.Now()
	peer.getNewBlockTimeout()
	peer.blockTimeStamp = timestamp
	peer.epochTimeStamp = timestamp
	peer.peersIds = peerId
	peer.record = generateRecordMap(peerId)
	peer.execNumber = *newOpsNumber(0, peer.id)
	peer.SmallBankInit(saving, savingAmount, checking, checkingAmount, publicKey, hashed, signature)
	return peer
}

// SmallBankInit Init, leveldb初始化，插入指定数量的key,value键值对
func (peer *Peer) SmallBankInit(saving []string, savingAmount []int, checking []string, checkingAmount []int, publicKey *rsa.PublicKey, hashed [32]byte, signature []byte) {
	peer.smallBank = NewSmallbank("leveldb"+strconv.Itoa(peer.id), saving, savingAmount, checking, checkingAmount, publicKey, hashed, signature)
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
		"	state: " + state + "\n"
	//"	block height: " + strconv.Itoa(peer.getBlockHeight()) + "\n}"
}

// 获取当前epoch中的{state: op->op}
func (peer *Peer) getHashTable(id int, bias int) map[string]StateSet {
	hashtable := make(map[string]StateSet)
	record, _ := peer.record[id]
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
	//fmt.Println("exec start...")
	//startTime := time.Now()
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
		//fmt.Print("exec time:")
		//fmt.Print(time.Since(startTime))
		//startTime = time.Now()
		peer.OperationAfterExecution(instances)
		//fmt.Print(" abort time:")
		//fmt.Print(time.Since(startTime))
		peer.mu.Lock()
		tmpSum := 0
		for _, id := range peer.peersIds {
			record4id, _ := peer.record[id]
			record4id.index += epoch[id]
			tmpSum += epoch[id]
			peer.record[id] = record4id
		}
		//fmt.Print("total block:")
		//fmt.Println(tmpSum)
		peer.mu.Unlock()
	}

}

// execInParalleling 并行执行不同Instance中的区块
func (peer *Peer) execInParalleling(ExecBlocks map[int][]Block) {
	var wg sync.WaitGroup
	wg.Add(len(ExecBlocks))
	for _, blocks := range ExecBlocks {
		tmpBlocks := blocks
		//tmpBuffer := buffer
		go func(blocks []Block, wg *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < len(blocks); i++ {
				var buffer sync.Map
				var wg4tx sync.WaitGroup
				wg4tx.Add(len(blocks[i].txs))
				//startTime := time.Now()
				for _, transaction := range blocks[i].txs {
					tmpTx := transaction
					//tx := transaction
					go func(tx *Tx, wg4tx *sync.WaitGroup) {
						defer wg4tx.Done()
						err := rsa.VerifyPKCS1v15(tx.publicKey, crypto.SHA256, tx.hashed[:], tx.signature)
						////fmt.Print("very time:")
						////fmt.Println(time.Since(startTime))
						if err != nil {
							panic(err)
						}
						switch tx.txType {
						case transactSavings:
							readOp := tx.Ops[0]
							writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
							// 第一个块读取数据库内容
							if i == 0 {
								readResult, _ := strconv.Atoi(peer.smallBank.Read(readOp.Key))
								WriteResult := readResult + writeValue
								tx.Ops[1].Val = strconv.Itoa(WriteResult) // 这里用于后续更新数据库execLastWrite
								buffer.Store(readOp.Key, WriteResult)
								//buffer[readOp.Key] = WriteResult
							} else {
								// 后续块读取前一个块的结果，如果没有buffer读取数据库
								readResult, exist := buffer.Load(readOp.Key)
								//readResult, exist := buffer[readOp.Key]
								if !exist {
									readResult, _ = strconv.Atoi(peer.smallBank.Read(readOp.Key))
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
								readResult, _ := strconv.Atoi(peer.smallBank.Read(readOp.Key))
								WriteResult := readResult + writeValue
								tx.Ops[1].Val = strconv.Itoa(WriteResult) // 这里用于后续更新数据库execLastWrite
								buffer.Store(readOp.Key, WriteResult)
								//buffer[readOp.Key] = WriteResult
							} else {
								readResult, exist := buffer.Load(readOp.Key)
								if !exist {
									readResult, _ = strconv.Atoi(peer.smallBank.Read(readOp.Key))
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
								readResultA, _ := strconv.Atoi(peer.smallBank.Read(readOpA.Key))
								readResultB, _ := strconv.Atoi(peer.smallBank.Read(readOpB.Key))
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
									readResultA, _ = strconv.Atoi(peer.smallBank.Read(readOpA.Key))
								}
								readResultB, exist := buffer.Load(readOpB.Key)
								if !exist {
									readResultB, _ = strconv.Atoi(peer.smallBank.Read(readOpB.Key))
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
								readResult, _ := strconv.Atoi(peer.smallBank.Read(readOp.Key))
								WriteResult := readResult + writeValue
								tx.Ops[1].Val = strconv.Itoa(WriteResult) // 这里用于后续更新数据库execLastWrite
								buffer.Store(readOp.Key, WriteResult)
								//buffer[readOp.Key] = WriteResult
							} else {
								readResult, exist := buffer.Load(readOp.Key)
								if !exist {
									readResult, _ = strconv.Atoi(peer.smallBank.Read(readOp.Key))
								}
								WriteResult := readResult.(int) + writeValue
								tx.Ops[1].Val = strconv.Itoa(WriteResult)
								//buffer.Store(readOp.Key, WriteResult)
							}
						case query:
							readOpSaving := tx.Ops[0]
							readOpChecking := tx.Ops[1]
							if i == 0 {
								peer.smallBank.Read(readOpSaving.Key)
								peer.smallBank.Read(readOpChecking.Key)
							} else {
								_, exist := buffer.Load(readOpSaving.Key)
								if !exist {
									peer.smallBank.Read(readOpSaving.Key)
								}
								_, exist = buffer.Load(readOpChecking.Key)
								if !exist {
									peer.smallBank.Read(readOpChecking.Key)
								}
							}
						case amalgamate:
							readOpSaving := tx.Ops[0]
							readOpChecking := tx.Ops[1]
							if i == 0 {
								readResultSaving, _ := strconv.Atoi(peer.smallBank.Read(readOpSaving.Key))
								WriteResultSaving := 0
								tx.Ops[2].Val = strconv.Itoa(WriteResultSaving)
								buffer.Store(readOpSaving.Key, 0)
								readResultChecking, _ := strconv.Atoi(peer.smallBank.Read(readOpChecking.Key))
								writeResultChecking := readResultSaving + readResultChecking
								tx.Ops[3].Val = strconv.Itoa(writeResultChecking)
								buffer.Store(readOpChecking.Key, writeResultChecking)
							} else {
								readResultSaving, exist := buffer.Load(readOpSaving.Key)
								if !exist {
									readResultSaving, _ = strconv.Atoi(peer.smallBank.Read(readOpSaving.Key))
								}
								readResultChecking, exist := buffer.Load(readOpChecking.Key)
								if !exist {
									readResultChecking, _ = strconv.Atoi(peer.smallBank.Read(readOpChecking.Key))
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
				//fmt.Print("each block:")
				//fmt.Println(time.Since(startTime))
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
	//startTime := time.Now()
	for _, instance := range instances {
		tmpInstance := instance
		go func(instance Instance, wg4computeCascade *sync.WaitGroup) {
			defer wg4computeCascade.Done()
			instance.computeCascade() // 计算每个instance的级联度
		}(tmpInstance, &wg4computeCascade)
	}
	wg4computeCascade.Wait()
	//fmt.Print("compute Cascade:")
	//fmt.Println(time.Since(startTime))
	//startTime = time.Now()
	// 获取所有address所对应的Instances,用于排序
	OrderInstanceMap := make(map[string]*OrderInstance, 0)
	instanceDict := make(map[int]int, 0) // 对应有向图坐标
	tmpIndex := 0
	for _, instance := range instances {
		instanceDict[instance.peerId] = tmpIndex
		tmpIndex++
		for address, _ := range instance.cascade {
			_, ok := OrderInstanceMap[address]
			if !ok {
				OrderInstanceMap[address] = newOrderInstance(address)
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
	//fmt.Print("address sort:")
	//fmt.Println(time.Since(startTime))
	//startTime = time.Now()
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
	//fmt.Print("Instance Sort:")
	//fmt.Println(time.Since(startTime))
	for _, orderInstance := range OrderInstanceMap {
		tmpInstance := orderInstance
		//fmt.Println("start go func....")
		//go func(tmpInstance OrderInstance, execWg *sync.WaitGroup) {
		//	defer execWg.Done()
		tmpInstance.OrderByDAG(topologicalOrder, instanceDict)
		flag, lastWrite := tmpInstance.execLastWrite()
		if flag {
			peer.smallBank.Update(lastWrite.op.Key, lastWrite.op.Val)
		}
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

//func (peer *Peer) log(content string) {
//	//var content = "(Log)" + peer.string()
//	file, err := os.OpenFile("log/peer_"+strconv.Itoa(peer.id)+".log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.FileMode(0777))
//	if err != nil {
//		fmt.Println(err)
//	}
//	defer file.Close()
//	_, err = file.WriteString(content + "\n")
//	if err != nil {
//		fmt.Println(err)
//	}
//}

// 获取块高
func (peer *Peer) getBlockHeight(id int) int {
	//fmt.Println(peer.RecordLog())
	return len(peer.record[id].blocks) - peer.record[id].index
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
	record4id := peer.record[id]
	record4id.index += bias
	peer.record[id] = record4id
}

// 更新出块timeout
func (peer *Peer) getNewBlockTimeout() {
	if peer.id != 3 {
		peer.blockTimeout = time.Duration(100) * time.Millisecond
	} else {
		peer.blockTimeout = time.Duration(400) * time.Millisecond
	}
	peer.blockTimeStamp = time.Now()
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
func (peer *Peer) BlockOut(flag int) {
	for i, _ := range peer.peersIds {
		if i != 3 {
			var tx = peer.smallBank.GenTxSet(config.BatchTxNum)
			//peer.log("generate tx:" + strconv.Itoa(len(tx)))
			newBlock := NewBlock(tx)
			peer.AppendBlockToRecord(i, *newBlock)
		}
	}
	if flag%400 == 0 {
		var tx = peer.smallBank.GenTxSet(config.BatchTxNum)
		//peer.log("generate tx:" + strconv.Itoa(len(tx)))
		newBlock := NewBlock(tx)
		peer.AppendBlockToRecord(peer.peersIds[3], *newBlock)
	}
	//for _, eachPeer := range peerMap {
	//eachPeer.AppendBlockToRecord(peer.id, *newBlock)
	//}
	//peer.log(peer.string())

}
func (peer *Peer) sendCheckBlockHeight(id int) int {
	return peer.getBlockHeight(id)
}

// 启动节点 Mine模式
func (peer *Peer) run() {
	for {
		if peer.state == Dead {
			break
		}
		if peer.state == Monitor {
			if peer.checkEpochTimeout() {
				//peer.log(peer.RecordLog())
				var heightMap map[int]int
				heightMap = make(map[int]int)
				//peer.log("Monitor(id:" + strconv.Itoa(peer.id) + "） send message to check block height to peers...")
				total := 0
				//time.Sleep(time.Duration(100) * time.Millisecond) // 得到实时树高耗时
				for _, id := range peer.peersIds {
					var height = peer.sendCheckBlockHeight(id)
					//fmt.Println(height)
					if height == 0 {
						continue
					}
					heightMap[id] = height
					total += height
				}
				if total == 0 {
					continue
				}
				if total > 10 {
					for id, height := range heightMap {
						if height == 0 {
							continue
						} else {
							tmp := int(math.Floor(float64(10) * float64(height) / float64(total)))
							if tmp == 0 {
								heightMap[id] = 1
							} else if tmp > 11-config.PeerNumber {
								heightMap[id] = 11 - config.PeerNumber
							} else {
								heightMap[id] = tmp
							}

						}
					}
				}
				//time.Sleep(time.Duration(100) * time.Millisecond) // 告诉其它节点epoch节点耗时
				//var wg sync.WaitGroup
				//wg.Add(len(peer.peersIds))
				// 根据heightMap得到各个节点剩余块高，然后计算epoch中的比例
				peer.exec(heightMap)
				//for _, eachPeer := range peerMap {
				//	tmp := eachPeer
				//	go func(tmp *Peer, wg *sync.WaitGroup) {
				//		defer wg.Done()
				//		tmp.exec(heightMap)
				//	}(tmp, &wg)
				//	//eachPeer.exec(heightMap)
				//}
				//wg.Wait()

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
func (peer *Peer) execInSequentialImpl(blocks []Block) {
	for _, block := range blocks {
		//time.Sleep(time.Duration(10) * time.Millisecond)
		//fmt.Println(len(block.txs))
		for _, tx := range block.txs {
			//fmt.Println("start verify...")
			//startTime := time.Now()
			err := rsa.VerifyPKCS1v15(tx.publicKey, crypto.SHA256, tx.hashed[:], tx.signature)
			if err != nil {
				panic(err)
			}
			//fmt.Println(time.Since(startTime))
			switch tx.txType {
			case transactSavings:
				readOp := tx.Ops[0]
				writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
				readResult, _ := strconv.Atoi(peer.smallBank.Read(readOp.Key))
				WriteResult := readResult + writeValue
				tx.Ops[1].Val = strconv.Itoa(WriteResult)
				peer.smallBank.Update(readOp.Key, strconv.Itoa(WriteResult))
			case depositChecking:
				readOp := tx.Ops[0]
				writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
				readResult, _ := strconv.Atoi(peer.smallBank.Read(readOp.Key))
				WriteResult := readResult + writeValue
				tx.Ops[1].Val = strconv.Itoa(WriteResult)
				peer.smallBank.Update(readOp.Key, strconv.Itoa(WriteResult))
			case sendPayment:
				readOpA := tx.Ops[0]
				readOpB := tx.Ops[1]
				writeValueA, _ := strconv.Atoi(tx.Ops[2].Val)
				writeValueB, _ := strconv.Atoi(tx.Ops[3].Val)
				readResultA, _ := strconv.Atoi(peer.smallBank.Read(readOpA.Key))
				readResultB, _ := strconv.Atoi(peer.smallBank.Read(readOpB.Key))
				WriteResultA := readResultA + writeValueA
				WriteResultB := readResultB + writeValueB
				tx.Ops[2].Val = strconv.Itoa(WriteResultA)
				peer.smallBank.Update(readOpA.Key, strconv.Itoa(writeValueA))
				tx.Ops[3].Val = strconv.Itoa(WriteResultB)
				peer.smallBank.Update(readOpB.Key, strconv.Itoa(writeValueB))
			case writeCheck:
				readOp := tx.Ops[0]
				writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
				readResult, _ := strconv.Atoi(peer.smallBank.Read(readOp.Key))
				WriteResult := readResult + writeValue
				tx.Ops[1].Val = strconv.Itoa(WriteResult)
				peer.smallBank.Update(readOp.Key, strconv.Itoa(WriteResult))
			case query:
				readOpSaving := tx.Ops[0]
				readOpChecking := tx.Ops[1]
				peer.smallBank.Read(readOpSaving.Key)
				peer.smallBank.Read(readOpChecking.Key)
			case amalgamate:
				readOpSaving := tx.Ops[0]
				readOpChecking := tx.Ops[1]
				readResultSaving, _ := strconv.Atoi(peer.smallBank.Read(readOpSaving.Key))
				readResultChecking, _ := strconv.Atoi(peer.smallBank.Read(readOpChecking.Key))
				writeResultSaving := 0
				tx.Ops[2].Val = strconv.Itoa(writeResultSaving)
				peer.smallBank.Update(readOpSaving.Key, strconv.Itoa(0))
				writeResultChecking := readResultSaving + readResultChecking
				tx.Ops[3].Val = strconv.Itoa(writeResultChecking)
				peer.smallBank.Update(readOpChecking.Key, strconv.Itoa(writeResultChecking))
			}
		}
	}
}
func (peer *Peer) execInSequential() {
	blocks := make([]Block, 0)
	for _, record := range peer.record {
		block := record.blocks[record.index]
		blocks = append(blocks, block)
	}
	// 执行交易
	peer.execInSequentialImpl(blocks)
	//peer.log("exec ops:" + strconv.Itoa(len(peer.peersIds)*config.BatchTxNum*config.OpsPerTx))
	for _, id := range peer.peersIds {
		record4id := peer.record[id]
		record4id.index += 1
		peer.record[id] = record4id
	}
	peer.execNumber.number += len(peer.peersIds) * config.BatchTxNum
	//peer.NotExecBlockIndex += epoch[peer.id]
}

func (peer *Peer) execInDoubleDetectImpl(blocks []Block) {
	if peer.id != 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(blocks))
	for _, block := range blocks {
		tmpTxs := block.txs
		go func(txs []*Tx, wg *sync.WaitGroup) {
			defer wg.Done()
			var wg4tx sync.WaitGroup
			wg4tx.Add(len(txs))
			for _, tx := range txs {
				tmpTx := tx
				go func(tx *Tx, wg4tx *sync.WaitGroup) {
					defer wg4tx.Done()
					err := rsa.VerifyPKCS1v15(tx.publicKey, crypto.SHA256, tx.hashed[:], tx.signature)
					////fmt.Print("very time:")
					////fmt.Println(time.Since(startTime))
					if err != nil {
						panic(err)
					}
					switch tx.txType {
					case transactSavings:
						readOp := tx.Ops[0]
						writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
						readResult, _ := strconv.Atoi(peer.smallBank.Read(readOp.Key))
						WriteResult := readResult + writeValue
						tx.Ops[1].Val = strconv.Itoa(WriteResult) // 这里用于后续更新数据库execLastWrite
						//buffer[readOp.Key] = WriteResult
					case depositChecking:
						readOp := tx.Ops[0]
						writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
						readResult, _ := strconv.Atoi(peer.smallBank.Read(readOp.Key))
						WriteResult := readResult + writeValue
						tx.Ops[1].Val = strconv.Itoa(WriteResult) // 这里用于后续更新数据库execLastWrite
					case sendPayment:
						readOpA := tx.Ops[0]
						readOpB := tx.Ops[1]
						writeValueA, _ := strconv.Atoi(tx.Ops[2].Val)
						writeValueB, _ := strconv.Atoi(tx.Ops[3].Val)
						readResultA, _ := strconv.Atoi(peer.smallBank.Read(readOpA.Key))
						readResultB, _ := strconv.Atoi(peer.smallBank.Read(readOpB.Key))
						WriteResultA := readResultA + writeValueA
						WriteResultB := readResultB + writeValueB
						tx.Ops[2].Val = strconv.Itoa(WriteResultA)
						tx.Ops[3].Val = strconv.Itoa(WriteResultB)
					case writeCheck:
						readOp := tx.Ops[0]
						writeValue, _ := strconv.Atoi(tx.Ops[1].Val)
						readResult, _ := strconv.Atoi(peer.smallBank.Read(readOp.Key))
						WriteResult := readResult + writeValue
						tx.Ops[1].Val = strconv.Itoa(WriteResult) // 这里用于后续更新数据库execLastWrite
					case query:
						readOpSaving := tx.Ops[0]
						readOpChecking := tx.Ops[1]
						peer.smallBank.Read(readOpSaving.Key)
						peer.smallBank.Read(readOpChecking.Key)
					case amalgamate:
						readOpSaving := tx.Ops[0]
						readOpChecking := tx.Ops[1]
						readResultSaving, _ := strconv.Atoi(peer.smallBank.Read(readOpSaving.Key))
						WriteResultSaving := 0
						tx.Ops[2].Val = strconv.Itoa(WriteResultSaving)
						readResultChecking, _ := strconv.Atoi(peer.smallBank.Read(readOpChecking.Key))
						writeResultChecking := readResultSaving + readResultChecking
						tx.Ops[3].Val = strconv.Itoa(writeResultChecking)
					}
				}(tmpTx, &wg4tx)
			}
		}(tmpTxs, &wg)
	}
}
func (peer *Peer) DoubleDetectConflict(blocks []Block) {
	AllTx := make([]*Tx, 0)
	DAG := make([][]int, len(AllTx)) // 有向图邻接矩阵
	for i := range DAG {
		DAG[i] = make([]int, len(AllTx))
		for j := range DAG[i] {
			DAG[i][j] = 0
		}
	}
	for _, block := range blocks {
		AllTx = append(AllTx, block.txs...)
	}
	for i := 0; i < len(AllTx); i++ {
		txA := AllTx[i]
		if txA.abort {
			continue
		}
		for j := i + 1; j < len(AllTx); j++ {
			txB := AllTx[j]
			if txB.abort {
				continue
			}
			ReadA := make(map[string]bool)
			WriteA := make(map[string]bool)
			for _, op := range txA.Ops {
				if op.Type == OpRead {
					_, exist := ReadA[op.Key]
					if !exist {
						ReadA[op.Key] = true
					}
				} else {
					_, exist := WriteA[op.Key]
					if !exist {
						WriteA[op.Key] = true
					}
				}
			}
			for _, op := range txB.Ops {
				if op.Type == OpRead {
					_, conflict := WriteA[op.Key]
					if conflict {
						txB.abort = true
						//conflictFlag4RW = true
						//DAG[i][j] = 1
					}
				} else {
					_, conflict := ReadA[op.Key]
					if conflict {
						txB.abort = true
						//DAG[j][i] = 1
					}
				}
			}
		}
	}
}
func (peer *Peer) execInDoubleDetect() {
	blocks := make([]Block, 0)
	for _, record := range peer.record {
		block := record.blocks[record.index]
		blocks = append(blocks, block)
	}
	// 执行交易
	peer.execInDoubleDetectImpl(blocks)
	peer.DoubleDetectConflict(blocks)
	for _, id := range peer.peersIds {
		record4id := peer.record[id]
		record4id.index += 1
		peer.record[id] = record4id
	}
	//peer.execNumber.number += len(peer.peersIds) * config.BatchTxNum
}
func (peer *Peer) runInParalleling() {
	for {
		if peer.state == Dead {
			break
		}
		if peer.checkComplete() {
			//peer.log(peer.RecordLog())
			//startTime := time.Now()
			peer.execInDoubleDetect()
			//fmt.Println(time.Since(startTime))
		}
	}
}

// 启动节点 Sequential模式
func (peer *Peer) runInSequential() {
	for {
		if peer.state == Dead {
			break
		}
		if peer.checkComplete() {
			//peer.log(peer.RecordLog())
			startTime := time.Now()
			peer.execInSequential()
			fmt.Println(time.Since(startTime))
		}
	}
}
func (peer *Peer) start() {
	fmt.Println("Peer(id:" + strconv.Itoa(peer.id) + ") start...")
	//peer.log("Peer(id:" + strconv.Itoa(peer.id) + ") start...")
	//peer.log(peer.string())
	go func(peer *Peer) {
		blockFlag := 100
		for {
			if peer.state == Dead {
				break
			}
			if peer.checkBlockTimeout() {
				peer.BlockOut(blockFlag)
				blockFlag += 100
				peer.getNewBlockTimeout()
			}
			//time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}(peer)
	switch config.RunType {
	case Mine:
		peer.run()
	case Sequential:
		peer.runInSequential()
	case Paralleling:
		peer.runInParalleling()
	}

}

// 停止节点
func (peer *Peer) stop() {
	peer.mu.Lock()
	peer.state = Dead
	//peer.log("Peer(id:" + strconv.Itoa(peer.id) + ") Dead...")
	fmt.Println("Peer(id:" + strconv.Itoa(peer.id) + ") Dead...")
	peer.mu.Unlock()
}
