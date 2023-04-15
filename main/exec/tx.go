package exec

import "crypto/rsa"

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
	txType    TxType // 交易类型
	Ops       []Op   // 交易中包含的操作
	abort     bool   // 是否abort
	sequence  int    // sorting时的序列号
	publicKey *rsa.PublicKey
	hashed    [32]byte
	signature []byte
}
