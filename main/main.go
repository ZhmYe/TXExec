package main

import "TXExec/exec"

func testParalleling() {
	exec.Init()
	exec.PeerInit(exec.Paralleling)
}
func testWaiting() {
	exec.Init()
	exec.PeerInit(exec.Waiting)
}
func main() {
	testParalleling()
	testWaiting()
	//exec.Test()
}
