package exec

type BlockState int

const (
	Finished BlockState = iota
	UnFinished
)

type Block struct {
	//mu    sync.Mutex // 并发
	txs   []*Tx      // 交易
	state BlockState // 区块是否已被执行
	//hash  string     // 区块哈希
}

func NewBlock(txs []*Tx) *Block {
	block := new(Block)
	//block.mu.Lock()
	block.txs = txs
	block.state = UnFinished
	//block.mu.Unlock()
	return block

}

// UpdateState 更改区块状态为已执行
func (block *Block) UpdateState() {
	//block.mu.Lock()
	block.state = Finished
	//block.mu.Unlock()
}
