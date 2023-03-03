package main

import "TXExec/exec"

func testParalleling() {
	exec.PeerInit(exec.Paralleling)
}
func testWaiting() {
	exec.PeerInit(exec.Waiting)
}
func main() {
	exec.Init()
	testParalleling()
	//testWaiting()
	//exec.Test()
}
