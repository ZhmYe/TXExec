package exec

import "time"

type Config struct {
	BatchTxNum     int // 每个Batch中交易的数量
	ValueSize      int
	OpsPerTx       int     // 每笔交易中操作的数量
	OriginKeys     int     // 初始Key的数量
	WRate          float64 // 写操作的比例
	HotKey         float64
	HotKeyRate     float64
	StdDiff        float64
	PeerNumber     int           // 节点数量
	execTimeout    time.Duration // 运行时间
	execTimeNumber int           // 运行时间数值
}

var config = Config{
	BatchTxNum:     10000,
	ValueSize:      64,
	OpsPerTx:       3,
	OriginKeys:     100000,
	WRate:          0.5,
	HotKey:         0.2,
	HotKeyRate:     0.6,
	StdDiff:        10000.0,
	PeerNumber:     4,
	execTimeout:    time.Duration(1) * time.Minute,
	execTimeNumber: 60,
}
