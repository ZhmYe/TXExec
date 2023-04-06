package exec

import "math"

type StateSet struct {
	ReadSet  []Unit // 读集
	WriteSet []Unit // 写集
}

func newStateSet() *StateSet {
	set := new(StateSet)
	set.ReadSet = make([]Unit, 0)
	set.WriteSet = make([]Unit, 0)
	return set
}

func (stateSet *StateSet) appendToReadSet(unit Unit) {
	stateSet.ReadSet = append(stateSet.ReadSet, unit)
}
func (stateSet *StateSet) appendToWriteSet(unit Unit) {
	stateSet.WriteSet = append(stateSet.WriteSet, unit)
}

type Instance struct {
	peerId        int                          // Instance ID
	hashTableList []map[string]StateSet        // 串联的子块hashtable
	cascade       map[string]int               // 级联度
	record        map[string]map[string][]Unit // 统计每笔交易在每个地址后面直接相连的读操作个数
}

func newInstance(id int) *Instance {
	instance := new(Instance)
	instance.peerId = id
	instance.hashTableList = make([]map[string]StateSet, 0) // 每个hashtable内通过key去寻找下一个元素内的顺序，保证顺序为rw
	instance.cascade = make(map[string]int, 0)
	instance.record = make(map[string]map[string][]Unit, 0)
	return instance
}
func (instance *Instance) addHashTable(hashtable map[string]StateSet) {
	instance.hashTableList = append(instance.hashTableList, hashtable)
}

// 计算级联度
// Instance hashtable结构:
// map[address] -> R-W-R-W
// 每个R-W对应一个StateSet,每个R或者W都是[]Unit,每个Unit包含*tx, tx_hash, op(type,key, val)
// 计算去掉第一个R以后其中abort的交易所涉及的写操作后面的读操作有几个
func (instance *Instance) computeCascade() {
	// nextReadNumberInAddress 统计每笔交易在每个地址后面直接相连的读操作个数
	nextReadNumberInAddress := make(map[string]map[string][]Unit, 0) // map[tx_hash] -> map[address] nextReadSet
	for i, hashtable := range instance.hashTableList {
		// 遍历hashtable中的每个address得到对应的stateset
		// 每个stateset的writeset的最后一个unit的tx_hash -> map[address] -> 下一个stateset中readset的长度
		// 下一个stateset通过继续向后面的hashtable寻找key得到，如果不存在key则向后寻找
		// 后续需考虑判断tx_hash是否出现在address后
		for address, stateset := range hashtable {
			writeSet := stateset.WriteSet
			tmpDistance := 0
			if len(writeSet) == 0 {
				continue
			}
			lastElement := writeSet[len(writeSet)-1] // lastElement.tx_hash
			// 可能是在块内并发已经被abort的，要取到没有被abort的最后一个
			tmpFlag := true
			for {
				if lastElement.tx.abort {
					tmpDistance += 1
					if len(writeSet)-1-tmpDistance < 0 {
						tmpFlag = false
						break
					}
					lastElement = writeSet[len(writeSet)-1-tmpDistance]
				} else {
					break
				}
			}
			// 说明该地址下所有的写集都已经被abort，无需进行后续的讨论
			if !tmpFlag {
				continue
			}
			_, inMap := nextReadNumberInAddress[lastElement.txHash]
			if !inMap {
				nextReadNumberInAddress[lastElement.txHash] = make(map[string][]Unit, 0)
			}
			flag := false
			for j := i + 1; j < len(instance.hashTableList); j++ {
				nextStateSet, exist := instance.hashTableList[j][address]
				// 如果下一个hashtable里包含了address，那么就记录其读集长度然后结束
				if exist {
					nextReadNumberInAddress[lastElement.txHash][address] = nextStateSet.ReadSet
					flag = true
					break
				}
			}
			// 如果没有后续hashtable
			if !flag {
				nextReadNumberInAddress[lastElement.txHash][address] = make([]Unit, 0)
			}
		}
	}
	instance.record = nextReadNumberInAddress
	// end for nextReadNumberInAddress

	// 计算instance每个address上第一个读集的级联度，所有交易在nextReadNumberInAddress中的所有address相加
	inRecord := make(map[string]bool) // 判断是否是每个address第一个读集
	for _, hashtable := range instance.hashTableList {
		for address, stateset := range hashtable {
			_, haveRecord := inRecord[address]
			// 是每个address的第一个读集
			if !haveRecord {
				inRecord[address] = true // 标记
				// 如果是第一个读集 才需要更新cascade变量
				instance.cascade[address] = getReadSetNumber(stateset.ReadSet, nextReadNumberInAddress)
			}
		}
	}

}
func getReadSetNumber(readSet []Unit, record map[string]map[string][]Unit) int {
	repeatCheck := make(map[string]bool)
	total := 0
	if len(readSet) == 0 {
		return 0
	}
	for _, unit := range readSet {
		_, repeat := repeatCheck[unit.txHash]
		// 如果当前交易有两笔读操作在同一个address或当前交易已经被abort,无需重复计算
		if repeat || unit.tx.abort {
			continue
		}
		total += 1
		repeatCheck[unit.txHash] = true
		CascadeInAddress, haveCascade := record[unit.txHash]
		if haveCascade {
			for _, eachReadSet := range CascadeInAddress {
				total += getReadSetNumber(eachReadSet, record)
			}
		}
	}
	return total
}
func (instance *Instance) abortReadSet(readSet []Unit) {
	repeatCheck := make(map[string]bool)
	if len(readSet) == 0 {
		return
	}
	for _, unit := range readSet {
		_, repeat := repeatCheck[unit.txHash]
		if repeat || unit.tx.abort {
			continue
		}
		repeatCheck[unit.txHash] = true
		unit.tx.abort = true
		CascadeInAddress, haveCascade := instance.record[unit.txHash]
		if haveCascade {
			for _, eachReadSet := range CascadeInAddress {
				instance.abortReadSet(eachReadSet)
			}
		}

	}
}

type OrderInstance struct {
	variance  float64    // 方差
	instances []Instance // 所有涉及的instance
	address   string     // 地址
}

func newOrderInstance(address string) *OrderInstance {
	orderInstance := new(OrderInstance)
	orderInstance.address = address
	orderInstance.variance = 0
	orderInstance.instances = make([]Instance, 0)
	return orderInstance
}
func (orderInstance *OrderInstance) appendInstance(instance Instance) {
	orderInstance.instances = append(orderInstance.instances, instance)
}
func (orderInstance *OrderInstance) computeVariance() {
	tmpList := make([]int, 0)
	tmpSum := 0
	for _, instance := range orderInstance.instances {
		tmpList = append(tmpList, instance.cascade[orderInstance.address])
		tmpSum += instance.cascade[orderInstance.address]
	}
	mean := float64(tmpSum) / float64(len(tmpList)) // 期望
	variance := float64(0)
	for _, cascade := range tmpList {
		variance += math.Pow(float64(cascade)-mean, 2)
	}
	orderInstance.variance = math.Sqrt(variance/float64(len(tmpList))) / mean

}
func (orderInstance *OrderInstance) getOrder() []int {
	result := make([]int, 0)
	// 冒泡排序, instances里的顺序就是最后Instance的顺序
	InstanceSortFlag := true
	for i := 0; i < len(orderInstance.instances)-1; i++ {
		InstanceSortFlag = true
		for j := 0; j < len(orderInstance.instances)-i-1; j++ {
			// 倒排 从大到小，级联度大的放在前面
			if orderInstance.instances[j].cascade[orderInstance.address] < orderInstance.instances[j+1].cascade[orderInstance.address] {
				orderInstance.instances[j], orderInstance.instances[j+1] = orderInstance.instances[j+1], orderInstance.instances[j]
				InstanceSortFlag = false
			}
		}
		if InstanceSortFlag {
			break
		}
	}
	for _, instance := range orderInstance.instances {
		result = append(result, instance.peerId)
	}
	return result
}
func (orderInstance *OrderInstance) OrderByDAG(sortOrder []int, indexDic map[int]int) {
	map4index2instance := make(map[int]Instance)
	for i := 0; i < len(orderInstance.instances)-1; i++ {
		map4index2instance[indexDic[orderInstance.instances[i].peerId]] = orderInstance.instances[i]
	}
	newInstances := make([]Instance, 0)
	for _, index := range sortOrder {
		instance, exist := map4index2instance[index]
		if exist {
			newInstances = append(newInstances, instance)
			if len(newInstances) == len(orderInstance.instances) {
				break
			}
		}
	}
	orderInstance.instances = newInstances
	//sortFlag := true
	//for i := 0; i < len(orderInstance.instances)-1; i++ {
	//	sortFlag = true
	//	for j := 0; j < len(orderInstance.instances)-i-1; j++ {
	//		if DAG[indexDic[orderInstance.instances[j].peerId]][indexDic[orderInstance.instances[j+1].peerId]] == 1 {
	//			orderInstance.instances[j], orderInstance.instances[j+1] = orderInstance.instances[j+1], orderInstance.instances[j]
	//			sortFlag = false
	//		}
	//	}
	//	if sortFlag {
	//		break
	//	}
	//}
	// 排好序后，除了第一个Instances外所有的Instances的第一个块的读集（有可能没有）需要全部abort，并将其级联的所有读abort
	if len(orderInstance.instances) > 1 {
		for i := 1; i < len(orderInstance.instances)-1; i++ {
			tmpInstance := orderInstance.instances[i]
			FirstBlock := tmpInstance.hashTableList[0]
			tmpInstance.abortReadSet(FirstBlock[orderInstance.address].ReadSet)
		}
	}

}
func (orderInstance *OrderInstance) execLastWrite() {
	lastWrite := *new(Unit)
	checkFlag := false
	for instanceIndex := len(orderInstance.instances) - 1; instanceIndex >= 0; instanceIndex-- {
		hashtableList := orderInstance.instances[instanceIndex].hashTableList
		for blockIndex := len(hashtableList) - 1; blockIndex >= 0; blockIndex-- {
			tmpStateSet, ok := hashtableList[blockIndex][orderInstance.address]
			if ok {
				flag := false
				for writeIndex := len(tmpStateSet.WriteSet) - 1; writeIndex >= 0; writeIndex-- {
					writeOp := tmpStateSet.WriteSet[writeIndex]
					if !writeOp.tx.abort {
						lastWrite = writeOp
						flag = true
						break
					}
				}
				if flag {
					checkFlag = true
					break
				}
			}
		}
		if checkFlag {
			break
		}
	}
	if checkFlag {
		Write(lastWrite.op.Key, lastWrite.op.Val)
	}
}
