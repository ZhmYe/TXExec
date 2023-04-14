package exec

import (
	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb"
	"math/rand"
	"strconv"
)

type Smallbank struct {
	savings   []string
	checkings []string
	db        *leveldb.DB
}

// TransactSavings 向储蓄账户增加一定余额
func (s *Smallbank) TransactSavings(account string, amount int) *Tx {
	r := Op{
		Type: OpRead,
		Key:  account,
	}
	w := Op{
		Type: OpWrite,
		Key:  account,
		Val:  strconv.Itoa(amount),
	}
	return &Tx{
		Ops:      []Op{r, w},
		abort:    false,
		sequence: -1,
	}
}

// DepositChecking 向支票账户增加一定余额
func (s *Smallbank) DepositChecking(account string, amount int) *Tx {
	r := Op{
		Type: OpRead,
		Key:  account,
	}
	w := Op{
		Type: OpWrite,
		Key:  account,
		Val:  strconv.Itoa(amount),
	}
	return &Tx{
		Ops: []Op{r, w},
	}
}

// SendPayment 在两个支票账户间转账
func (s *Smallbank) SendPayment(accountA string, accountB string, amount int) *Tx {
	ra := Op{
		Type: OpRead,
		Key:  accountA,
	}
	rb := Op{
		Type: OpRead,
		Key:  accountB,
	}
	wa := Op{
		Type: OpWrite,
		Key:  accountA,
		Val:  strconv.Itoa(-amount),
	}
	wb := Op{
		Type: OpWrite,
		Key:  accountB,
		Val:  strconv.Itoa(amount),
	}
	return &Tx{
		Ops: []Op{ra, rb, wa, wb},
	}
}

// WriteCheck 减少一个支票账户
func (s *Smallbank) WriteCheck(account string, amount int) *Tx {
	r := Op{
		Type: OpRead,
		Key:  account,
	}
	w := Op{
		Type: OpWrite,
		Key:  account,
		Val:  strconv.Itoa(-amount),
	}
	return &Tx{
		Ops: []Op{r, w},
	}
}

// Amalgamate 将储蓄账户的资金全部转到支票账户
func (s *Smallbank) Amalgamate(saving string, checking string) *Tx {
	ra := Op{
		Type: OpRead,
		Key:  saving,
	}
	rb := Op{
		Type: OpRead,
		Key:  checking,
	}
	wa := Op{
		Type: OpWrite,
		Key:  saving,
		Val:  strconv.Itoa(0),
	}
	wb := Op{
		Type: OpWrite,
		Key:  checking,
		Val:  strconv.Itoa(0),
	}
	return &Tx{
		Ops: []Op{ra, rb, wa, wb},
	}
}

// Query 查询第i个用户的saving和checking
func (s *Smallbank) Query(saving string, checking string) *Tx {
	ra := Op{
		Type: OpRead,
		Key:  saving,
	}
	rb := Op{
		Type: OpRead,
		Key:  checking,
	}
	return &Tx{
		Ops: []Op{ra, rb},
	}
}

func (s *Smallbank) GetRandomAmount() int {
	return RandomRange(1e3, 1e4)
}

func (s *Smallbank) GetNormalRandomIndex() int {
	n := len(s.savings)
	hotRateCheck := rand.Float64()
	if hotRateCheck < config.HotKeyRate {
		return int(rand.Float64() * float64(n) * config.HotKey)
	} else {
		return int(rand.Float64()*float64(n)*(1-config.HotKey)) + int(float64(n)*config.HotKey)
	}
	//for {
	//	x := int(rand.NormFloat64()*config.StdDiff) + n/2
	//	if x >= 0 && x < n {
	//		return x
	//	}
	//}
}

func (s *Smallbank) GetRandomTx() *Tx {
	r0 := rand.Float64()
	if r0 > config.WRate {
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

func (s *Smallbank) GenTxSet(n int) []*Tx {
	txs := make([]*Tx, n)
	for i := range txs {
		txs[i] = s.GetRandomTx()
	}
	return txs
}

// RandomRange [l, r)
func RandomRange(l, r int) int {
	return rand.Intn(r-l) + l
}

// Read 从leveldb中读
func (s *Smallbank) Read(key string) string {
	val, _ := s.db.Get([]byte(key), nil)
	return string(val)
}

// Write 向leveldb中写
func (s *Smallbank) Write(key, val string) {
	s.db.Put([]byte(key), []byte(val), nil)
}

// Update 更新leveldb的数据
func (s *Smallbank) Update(key, val string) {
	s.db.Put([]byte(key), []byte(val), nil)
}
func NewSmallbank(path string, n int) *Smallbank {
	// 为特定数量的用户创建一个支票账户和一个储蓄账户，第i个用户的储蓄金地址为savings[i],支票地址为checkings[i]
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
