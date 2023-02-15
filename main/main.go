package main

import (
	"TXExec/exec"
	"fmt"
	"os"
	"os/signal"
)

func main() {
	c := make(chan os.Signal)
	signal.Notify(c)
	exec.Init()
	<-c
	fmt.Println("bye")
}
