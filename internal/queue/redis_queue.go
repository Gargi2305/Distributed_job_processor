package queue

import (
	"context"
	"encoding/json"

	"github.com/Gargi2305/Distributed_job_processor/internal/config"
)

const (
	JobQueueName    = "job_queue"
	FailedJobsQueue = "failed_jobs"
)

func Enqueue(ctx context.Context, rdb *config.Client, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return rdb.LPush(ctx, JobQueueName, string(data))
}

func Requeue(ctx context.Context, rdb *config.Client, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return rdb.LPush(ctx, JobQueueName, string(data))
}

func MoveToFailed(ctx context.Context, rdb *config.Client, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return rdb.LPush(ctx, FailedJobsQueue, string(data))
}

func Pop(ctx context.Context, rdb *config.Client) (string, error) {
	return rdb.BRPop(ctx, 0, JobQueueName)
}
