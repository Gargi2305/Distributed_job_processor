package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Gargi2305/Distributed_job_processor/internal/config"
	"github.com/Gargi2305/Distributed_job_processor/internal/handlers"
	"github.com/Gargi2305/Distributed_job_processor/internal/models"
	"github.com/Gargi2305/Distributed_job_processor/internal/queue"
	"github.com/Gargi2305/Distributed_job_processor/internal/service"
	"github.com/Gargi2305/Distributed_job_processor/pkg/logger"
)

type Worker struct {
	ID     int
	Redis  *config.Client
	Ctx    context.Context
	Cancel context.CancelFunc
}

func NewWorker(id int, rdb *config.Client) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{ID: id, Redis: rdb, Ctx: ctx, Cancel: cancel}
}

func (w *Worker) Start() {
	logger.Info("worker_started", fmt.Sprintf("id=%d", w.ID))
	for {
		select {
		case <-w.Ctx.Done():
			return
		default:
			payload, err := queue.Pop(w.Ctx, w.Redis)
			if err != nil {
				if err == config.ErrNil {
					continue
				}
				logger.Error("job_failed", fmt.Sprintf("worker=%d err=%v", w.ID, err))
				continue
			}

			var job models.Job
			if err := json.Unmarshal([]byte(payload), &job); err != nil {
				logger.Error("job_failed", fmt.Sprintf("worker=%d err=%v", w.ID, err))
				continue
			}

			_ = service.UpdateStatus(w.Ctx, w.Redis, job.ID, models.StatusProcessing, "")
			logger.Info("job_started", fmt.Sprintf("id=%s worker=%d", job.ID, w.ID))

			result, err := handlers.Handle(&job)
			if err == nil {
				job.Result = result
				job.Status = models.StatusCompleted
				_ = service.UpdateStatus(w.Ctx, w.Redis, job.ID, models.StatusCompleted, job.Result)
				logger.Info("job_completed", fmt.Sprintf("id=%s worker=%d", job.ID, w.ID))
				continue
			}

			retries, rerr := service.IncrementRetry(w.Ctx, w.Redis, &job)
			if rerr != nil {
				logger.Error("job_failed", fmt.Sprintf("id=%s worker=%d err=%v", job.ID, w.ID, rerr))
				continue
			}

			job.Result = err.Error()
			if retries < service.MaxRetries() {
				_ = service.RequeueJob(w.Ctx, w.Redis, &job)
				logger.Info("job_retried", fmt.Sprintf("id=%s worker=%d retry=%d err=%v", job.ID, w.ID, retries, err))
				continue
			}

			_ = service.MoveToFailedQueue(w.Ctx, w.Redis, &job)
			_ = service.UpdateStatus(w.Ctx, w.Redis, job.ID, models.StatusFailed, job.Result)
			logger.Error("job_failed", fmt.Sprintf("id=%s worker=%d err=%v", job.ID, w.ID, err))
		}
	}
}
