package main

import "reaperes.xyz/gas-calculator/pkg/sync"

func main() {
	o := sync.NewSyncOperator()
	o.StartSync()
}
