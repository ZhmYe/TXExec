package exec

import (
	"time"
)

type Config struct {
	BatchTxNum   int // 每个Batch中交易的数量
	ValueSize    int
	OpsPerTx     int     // 每笔交易中操作的数量
	OriginKeys   int     // 初始Key的数量
	WRate        float64 // 写操作的比例
	HotKey       float64
	HotKeyRate   float64
	StdDiff      float64
	PeerNumber   int // 节点数量
	EpochTimeout time.Duration
}

var config = Config{
	BatchTxNum:   10000,
	ValueSize:    64,
	OpsPerTx:     3,
	OriginKeys:   100000,
	WRate:        0.5,
	HotKey:       0.2,
	HotKeyRate:   0.6,
	StdDiff:      10000.0,
	PeerNumber:   1,
	EpochTimeout: time.Duration(400) * time.Millisecond,
}
