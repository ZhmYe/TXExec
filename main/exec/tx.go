package exec

import (
	"fmt"
	"math/rand"
	"strconv"
)

var (
	//db   *leveldb.DB
	smallbank *Smallbank
	karr      []string // key array,由config中的originkeys数量生成
	// kmap map[string]int
)

// Init leveldb初始化，插入指定数量的key,value键值对并统计时间
func Init() {
	//var err error
	//db, err = leveldb.OpenFile("levdb", nil)
	//if err != nil {
	//	fmt.Println(err)
	//	panic("open db failed!")
	//}
	//// kmap = make(map[string]int, config.OriginKeys)
	//karr = make([]string, 0, config.OriginKeys) // 根据初始配置的key数量生成Key array
	//for i := 0; i <= config.OriginKeys; i++ {
	//	key := uuid.NewString() // 生成key,uuid类型
	//	Write(key, "")          // 向leveldb中插入key,value("")
	//	// kmap[key] = len(karr)
	//	karr = append(karr, key)
	//}
	fmt.Println("Init Smallbank Start...")
	smallbank = NewSmallbank("levdb", config.OriginKeys)
	fmt.Println("Original Address Number:" + strconv.Itoa(config.OriginKeys))
	fmt.Println("Init Smallbank Finished")
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
type TxType int

const (
	transactSavings TxType = iota
	depositChecking
	sendPayment
	writeCheck
	amalgamate
	query
)

// Tx 交易
type Tx struct {
	txType   TxType // 交易类型
	Ops      []Op   // 交易中包含的操作
	abort    bool   // 是否abort
	sequence int    // sorting时的序列号
}

// 根据热点率获取随机的key todo
func getRandomKeyWithHot() string {
	idx := getNormalRandom()
	return karr[idx]
}

// todo
func getNormalRandom() int {
	u := len(karr) / 2
	for {
		x := int(rand.NormFloat64()*config.StdDiff) + u
		if x >= 0 && x < len(karr) {
			return x
		}
	}
}
