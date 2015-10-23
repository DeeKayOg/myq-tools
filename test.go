package main

import (
	"fmt"
	"github.com/jayjanssen/myq-tools/myqlib"
	"time"
)

func main() {
	interval := time.Second
	loader := myqlib.NewSqlLoader(interval, "root", "", "")

	// Get channel that will feed us states from the loader
	states, err := myqlib.GetState(loader)
	if err != nil {
		fmt.Println(err)
	}

	for state := range states {
		fmt.Println(state.Cur[`uptime`])
	}
}
