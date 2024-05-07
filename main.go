package main

import "github.com/moveaxlab/oracle-db-job-queue/queue"

func main() {
	db := queue.NewOracleQueue()
	db.Migrate()
}
