package db

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
)

// TODO database url 변경해야 함
const DATABASE_URL = "postgresql://local:local@127.0.0.1:5432/point"

func CreateConnectionPool() *pgxpool.Pool {
	dbpool, err := pgxpool.New(context.Background(), DATABASE_URL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create connection pool: %v\n", err)
		os.Exit(1)
	}
	return dbpool
}
