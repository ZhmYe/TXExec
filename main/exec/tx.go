package exec

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"math/rand"
	"strconv"
)

var (
	db             *leveldb.DB
	karr           []string // key array,由config中的originkeys数量生成
	spliceLength   int      // 分片长度
	conflictLength int      // 公用长度
	// kmap map[string]int
)

// Init leveldb初始化，插入指定数量的key,value键值对并统计时间
func Init() {
	var err error
	db, err = leveldb.OpenFile("levdb", nil)
	if err != nil {
		fmt.Println(err)
		panic("open db failed!")
	}
	// kmap = make(map[string]int, config.OriginKeys)
	karr = make([]string, 0, config.OriginKeys) // 根据初始配置的key数量生成Key array
	for i := 0; i <= config.OriginKeys; i++ {
		key := uuid.NewString() // 生成key,uuid类型
		Write(key, "")          // 向leveldb中插入key,value("")
		// kmap[key] = len(karr)
		karr = append(karr, key)
	}
	if !config.ConflictMode { // 局部冲突
		// 每个splice公用conflictLength长度(l * r)的区间剩下的l * (1 -r)，一共 l * (1 - r) * n + l = len
		spliceLength = int(float64(len(karr)) / ((1-config.InstanceConflictRate)*float64(config.PeerNumber) + 1))
		conflictLength = int(float64(spliceLength) * config.InstanceConflictRate)
	} else { // 全局冲突
		spliceLength = len(karr)
		conflictLength = len(karr)
	}
}

// Read 从leveldb中读
func Read(key string) string {
	val, _ := db.Get([]byte(key), nil)
	return string(val)
}

// Write 向leveldb中写
func Write(key, val string) {
	db.Put([]byte(key), []byte(val), nil)
}

// Update 更新leveldb的数据
func Update(key, val string) {
	db.Put([]byte(key), []byte(val), nil)
}

// OpType 操作类型
type OpType int

const (
	OpRead  OpType = iota // 读操作
	OpWrite               // 写操作
	NumOfOpType
)

// Op 操作
type Op struct {
	Type OpType // 操作类型 读/写
	Key  string // 操作的key
	Val  string // 操作的value
}

// Tx 交易
type Tx struct {
	Ops      []Op // 交易中包含的操作
	abort    bool // 是否abort
	sequence int  // sorting时的序列号
}

// 根据热点率获取随机的key todo
func getRandomKeyWithHot(peerId int) string {
	//r := rand.Float64()
	//n := int(float64(len(karr)) * config.HotKey)
	//idx := 0
	//if r < config.HotKeyRate {
	//	idx = rand.Intn(n)
	//} else {
	//	idx = rand.Intn(len(karr)-n) + n
	//}
	//fmt.Println(111)
	idx := getNormalRandom(peerId)
	if idx < conflictLength {
		return karr[idx]
	}
	//fmt.Println(idx)
	return karr[idx+peerId*spliceLength]
}

// todo
func getNormalRandom(peerId int) int {
	u := spliceLength / 2
	for {
		x := int(rand.NormFloat64()*config.StdDiff) + u
		if x >= 0 && x < spliceLength {
			return x
		}
	}
}

// GenTxSet 生成交易
func GenTxSet(peerId int) []*Tx {
	n := config.BatchTxNum
	m := config.OpsPerTx
	valFormat := "%0" + strconv.Itoa(config.ValueSize) + "%s" // todo
	wrate := config.WRate
	txs := make([]*Tx, n)
	for i := range txs {
		ops := make([]Op, m)
		for j := range ops {
			r := rand.Float64()
			if r < wrate {
				// 生成一笔写操作
				ops[j].Type = OpWrite
				ops[j].Key = getRandomKeyWithHot(peerId)
				ops[j].Val = fmt.Sprintf(valFormat, uuid.NewString()) // todo
			} else {
				// 生成一笔读操作
				ops[j].Type = OpRead
				ops[j].Key = getRandomKeyWithHot(peerId)
			}
		}
		txs[i] = &Tx{Ops: ops, abort: false, sequence: -1}
	}
	return txs
}

// EncodeTxSet 将交易转化为bytes? todo
func EncodeTxSet(txs []*Tx) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(txs)
	return buf.Bytes()
}

// ExecTxSet 执行交易
func ExecTxSet(txSet []byte) int {
	var txs []*Tx
	buf := bytes.NewBuffer(txSet)
	dec := gob.NewDecoder(buf)
	dec.Decode(&txs)

	for _, tx := range txs {
		for _, op := range tx.Ops {
			if op.Type == OpRead {
				Read(op.Key)
			} else {
				Write(op.Key, op.Val)
			}
		}
	}
	return len(txs)
}
