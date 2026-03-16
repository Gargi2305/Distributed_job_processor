package main

import (
	"fmt"

	"github.com/Gargi2305/Distributed_job_processor/internal/config"
	"github.com/Gargi2305/Distributed_job_processor/internal/worker"
	"github.com/Gargi2305/Distributed_job_processor/pkg/logger"
)

func main() {
	rdb := config.NewRedisClient()
	pool := worker.NewWorkerPool(rdb)
	logger.Info("worker_pool_started", fmt.Sprintf("count=%d", pool.WorkerCount))
	pool.Start()
}
