package main

import (
	"TXExec/exec"
	"runtime"
)

func test() {
	//exec.ConflictTest()
	//exec.PeerInit()
	exec.SimpleTest()
}
func main() {
	runtime.GOMAXPROCS(8)
	//exec.Init()
	test()
}
