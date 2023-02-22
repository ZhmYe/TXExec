package main

import "TXExec/exec"

func main() {
	exec.Init()
	exec.PeerInit(exec.Paralleling)
	exec.PeerInit(exec.Waiting)
	//exec.Test()
}
