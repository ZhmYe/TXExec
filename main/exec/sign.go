package exec

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
)

func getSignInfo() (*rsa.PublicKey, [32]byte, []byte) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	publicKey := &privateKey.PublicKey

	// 待签名的数据
	message := []byte("hello world")

	// 计算消息摘要
	hashed := sha256.Sum256(message)

	// 对消息摘要进行数字签名
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hashed[:])
	if err != nil {
		panic(err)
	}
	return publicKey, hashed, signature
}
