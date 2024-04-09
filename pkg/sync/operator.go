package sync

import (
	"context"
	"fmt"
	"reaperes.xyz/gas-calculator/pkg/db"
	"reaperes.xyz/gas-calculator/pkg/dune"
)

const EXECUTION_ID = 3600529
const SYNC_LIMIT = 2000
const START_OFFSET = 0
const MAX_OFFSET = 106829323 // TODO 240408 updated, improve to allow for dynamic changes

type SyncOperator struct {
	duneClient *dune.DuneClient
}

func NewSyncOperator() *SyncOperator {
	o := SyncOperator{
		duneClient: dune.NewDuneClient(),
	}
	return &o
}

// TODO Current flow runs only once and then terminates. It needs to be improved to continuously update periodically
// TODO The query execution is not optimized. Necessary to check settings related to batch insert, connection pooling, parallel execution, connection acquire/release.
// TODO handle error
func (o *SyncOperator) StartSync() {
	offset := START_OFFSET
	maxRow := MAX_OFFSET
	maxOffset := maxRow - SYNC_LIMIT

	pool := db.CreateConnectionPool()
	defer pool.Close()

	for offset <= maxOffset {
		result, err := o.duneClient.GetExecutionResult(EXECUTION_ID, offset, SYNC_LIMIT)
		if err != nil {
			panic(err)
		}

		acquire, err := pool.Acquire(context.TODO())
		if err != nil {
			return
		}
		for i := 0; i < len(result.Result.Rows); i++ {
			query := `
INSERT INTO _dune_total_gas_used (address, total_gas_used, "updatedAt")
VALUES ($1, $2, NOW())
ON CONFLICT (address) DO
UPDATE
SET total_gas_used = $2, "updatedAt" = NOW()
`
			_, err := acquire.Exec(context.TODO(), query, fmt.Sprintf("%s", result.Result.Rows[i]["address"]), fmt.Sprintf("%.f", result.Result.Rows[i]["total_gas_used"]))
			if err != nil {
				panic(err)
			}
			//fmt.Printf("%s\n", out)
		}
		acquire.Release()
		fmt.Printf("offset completed: %d\n", offset)
		offset += SYNC_LIMIT
	}
}
