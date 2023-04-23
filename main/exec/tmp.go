package exec

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
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

func tmpGetSignInfo(testData test) (*rsa.PublicKey, [32]byte, []byte) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	publicKey := &privateKey.PublicKey

	// 待签名的数据
	//message := []byte("hello world")

	// 计算消息摘要
	message := []byte(fmt.Sprintf("%v", testData))
	hashed := sha256.Sum256(message)

	// 对消息摘要进行数字签名
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hashed[:])
	if err != nil {
		panic(err)
	}
	return publicKey, hashed, signature
}
func TmpTest() {
	testData := test{100, 10, 10}
	_, _, signature := tmpGetSignInfo(testData)
	//fmt.Println(Publickey, hashed, signature)
	testMsg := msg{testData, signature}
	fmt.Println(unsafe.Sizeof(testMsg))
}
