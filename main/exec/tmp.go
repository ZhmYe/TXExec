package exec

import (
	"fmt"
	"unsafe"
)

type test struct {
	workload int64
	nodeId   int64
	number   int64
}
type msg struct {
	data test
	sign []byte
}

func TmpTest() {
	_, _, signature := getSignInfo()
	//fmt.Println(Publickey, hashed, signature)
	testData := test{100, 10, 10}
	testMsg := msg{testData, signature}
	fmt.Print(unsafe.Sizeof(testMsg))
}
