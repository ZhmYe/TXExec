package main

import "TXExec/exec"

func testParalleling() {
	exec.PeerInit()
}
func main() {
	exec.Init()
	testParalleling()
}
