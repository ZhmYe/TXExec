package main

import (
	"TXExec/exec"
	"fmt"
	"runtime"
)

func test() {
	//exec.ConflictTest()
	//exec.PeerInit()
	exec.SimpleTest()
}
func main() {
	fmt.Println(runtime.NumCPU())
	runtime.GOMAXPROCS(8)
	//exec.Init()
	test()
}
