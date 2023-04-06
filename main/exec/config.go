package exec

import "time"

type Mode int

type Config struct {
	BatchTxNum           int // 每个Batch中交易的数量
	ValueSize            int
	OpsPerTx             int     // 每笔交易中操作的数量
	OriginKeys           int     // 初始Key的数量
	WRate                float64 // 写操作的比例
	ConflictMode         bool    // 冲突模式 全局(True)/局部
	InstanceConflictRate float64 // 局部冲突模式下，各个instance间冲突概率
	HotKey               float64
	HotKeyRate           float64 // 有HotKeyRate的交易访问HotKey的状态
	StdDiff              float64
	PeerNumber           int           // 节点数量
	execTimeout          time.Duration // 运行时间
	execTimeNumber       int           // 运行时间数值
}

var config = Config{
	BatchTxNum:           10000,
	ValueSize:            64,
	OpsPerTx:             3,
	OriginKeys:           100000, // 10000
	WRate:                0.5,
	ConflictMode:         false,
	InstanceConflictRate: 0.2,
	HotKey:               0.2,
	HotKeyRate:           0.6,
	StdDiff:              10000.0,
	PeerNumber:           4,
	execTimeout:          time.Duration(1) * time.Minute,
	execTimeNumber:       60,
}
