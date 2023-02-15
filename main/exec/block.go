package exec

type BlockState int

const (
	Finished BlockState = iota
	UnFinished
)

type Block struct {
	txs   []Tx       // 交易
	state BlockState // 区块是否已被执行
}

func NewBlock(txs []Tx) *Block {
	block := new(Block)
	block.txs = txs
	block.state = UnFinished
	return block

}

// UpdateState 更改区块状态为已执行
func (block *Block) UpdateState() {
	block.state = Finished
}
