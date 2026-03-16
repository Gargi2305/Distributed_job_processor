package worker

import (
	"os"
	"strconv"

	"github.com/Gargi2305/Distributed_job_processor/internal/config"
)

type WorkerPool struct {
	WorkerCount int
	Redis       *config.Client
}

func NewWorkerPool(rdb *config.Client) *WorkerPool {
	count := 5
	if v := os.Getenv("WORKER_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			count = n
		}
	}
	return &WorkerPool{WorkerCount: count, Redis: rdb}
}

func (p *WorkerPool) Start() {
	for i := 0; i < p.WorkerCount; i++ {
		w := NewWorker(i+1, p.Redis)
		go w.Start()
	}
	select {}
}
