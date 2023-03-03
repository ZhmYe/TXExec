package exec

type OrderTxs struct {
	order int  // 排序序列号
	txs   []Tx // 这一序列运行的交易
}

func newOrderTxs(order int, txs []Tx) *OrderTxs {
	orderTxs := new(OrderTxs)
	orderTxs.order = order
	orderTxs.txs = txs
	return orderTxs
}
func TransactionSort(hashtable map[string]StateSet) []OrderTxs {
	
}
