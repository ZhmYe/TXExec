package smallbank

import (
	"math/rand"
	"strconv"
	"zbenchmark/ycsb"

	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb"
)

type Smallbank struct {
	savings   []string
	checkings []string
	db        *leveldb.DB
}

func (s *Smallbank) TransactSavings(account string, amount int) *ycsb.Tx {
	r := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  account,
	}
	w := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  account,
		Val:  strconv.Itoa(amount),
	}
	return &ycsb.Tx{
		Ops: []ycsb.Op{r, w},
	}
}

func (s *Smallbank) DepositChecking(account string, amount int) *ycsb.Tx {
	r := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  account,
	}
	w := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  account,
		Val:  strconv.Itoa(amount),
	}
	return &ycsb.Tx{
		Ops: []ycsb.Op{r, w},
	}
}

func (s *Smallbank) SendPayment(accountA string, accountB string, amount int) *ycsb.Tx {
	ra := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  accountA,
	}
	rb := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  accountB,
	}
	wa := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  accountA,
		Val:  strconv.Itoa(-amount),
	}
	wb := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  accountB,
		Val:  strconv.Itoa(amount),
	}
	return &ycsb.Tx{
		Ops: []ycsb.Op{ra, rb, wa, wb},
	}
}

func (s *Smallbank) WriteCheck(account string, amount int) *ycsb.Tx {
	r := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  account,
	}
	w := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  account,
		Val:  strconv.Itoa(-amount),
	}
	return &ycsb.Tx{
		Ops: []ycsb.Op{r, w},
	}
}

func (s *Smallbank) Amalgamate(saving string, checking string) *ycsb.Tx {
	ra := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  saving,
	}
	rb := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  checking,
	}
	wa := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  saving,
		Val:  strconv.Itoa(0),
	}
	wb := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  checking,
		Val:  strconv.Itoa(rand.Intn(10e4) + 10e4),
	}
	return &ycsb.Tx{
		Ops: []ycsb.Op{ra, rb, wa, wb},
	}
}

func (s *Smallbank) Query(saving string, checking string) *ycsb.Tx {
	ra := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  saving,
	}
	rb := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  checking,
	}
	return &ycsb.Tx{
		Ops: []ycsb.Op{ra, rb},
	}
}

func (s *Smallbank) GetRandomAmount() int {
	return RandomRange(1e4, 1e5)
}

func (s *Smallbank) GetNormalRandomIndex() int {
	n := len(s.savings)
	for {
		x := int(rand.NormFloat64()*ycsb.KConfig.StdDiff) + n/2
		if x >= 0 && x < n {
			return x
		}
	}
}

func (s *Smallbank) GetRandomTx() *ycsb.Tx {
	r0 := rand.Float64()
	if r0 > ycsb.KConfig.WRate {
		i := s.GetNormalRandomIndex()
		return s.Query(s.savings[i], s.checkings[i])
	}
	switch rand.Int() % 5 {
	case 0:
		i := s.GetNormalRandomIndex()
		amount := s.GetRandomAmount()
		return s.TransactSavings(s.savings[i], amount)
	case 1:
		i := s.GetNormalRandomIndex()
		amount := s.GetRandomAmount()
		return s.DepositChecking(s.checkings[i], amount)
	case 2:
		i := s.GetNormalRandomIndex()
		j := s.GetNormalRandomIndex()
		for j == i {
			j = s.GetNormalRandomIndex()
		}
		amount := s.GetRandomAmount()
		return s.SendPayment(s.checkings[i], s.checkings[j], amount)
	case 3:
		i := s.GetNormalRandomIndex()
		amount := s.GetRandomAmount()
		return s.WriteCheck(s.checkings[i], amount)
	case 4:
		i := s.GetNormalRandomIndex()
		return s.Amalgamate(s.savings[i], s.checkings[i])
	}
	panic("err")
}

func (s *Smallbank) GenTxSet(n int) []*ycsb.Tx {
	txs := make([]*ycsb.Tx, n)
	for i := range txs {
		txs[i] = s.GetRandomTx()
	}
	return txs
}

// [l, r)
func RandomRange(l, r int) int {
	return rand.Intn(r-l) + l
}

func NewSmallbank(path string, n int) *Smallbank {
	s := &Smallbank{
		savings:   make([]string, n),
		checkings: make([]string, n),
	}
	var err error
	s.db, err = leveldb.OpenFile(path, nil)
	if err != nil {
		panic("open leveldb failed!")
	}
	for i := range s.savings {
		s.savings[i] = uuid.NewString()
		s.checkings[i] = uuid.NewString()
		savingAmount := RandomRange(1e4, 1e5)
		checkingAmount := RandomRange(1e3, 1e4)
		s.db.Put([]byte(s.savings[i]), []byte(strconv.Itoa(savingAmount)), nil)
		s.db.Put([]byte(s.checkings[i]), []byte(strconv.Itoa(checkingAmount)), nil)
	}
	return s
}
