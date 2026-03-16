package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/Gargi2305/Distributed_job_processor/internal/config"
	"github.com/Gargi2305/Distributed_job_processor/internal/models"
	"github.com/Gargi2305/Distributed_job_processor/internal/queue"
	"github.com/Gargi2305/Distributed_job_processor/pkg/logger"
)

const jobKeyPrefix = "job:"

func maxRetries() int {
	val := os.Getenv("MAX_RETRIES")
	if val == "" {
		return 3
	}
	n, err := strconv.Atoi(val)
	if err != nil || n < 1 {
		return 3
	}
	return n
}

func CreateJob(ctx context.Context, rdb *config.Client, jobType string, payload string) (*models.Job, error) {
	job := &models.Job{
		ID:      newJobID(),
		Type:    jobType,
		Payload: payload,
		Status:  models.StatusQueued,
		Retries: 0,
		Result:  "",
	}

	if err := saveJob(ctx, rdb, job); err != nil {
		return nil, err
	}
	if err := queue.Enqueue(ctx, rdb, job); err != nil {
		return nil, err
	}

	logger.Info("job_received", fmt.Sprintf("id=%s type=%s", job.ID, job.Type))
	return job, nil
}

func GetJob(ctx context.Context, rdb *config.Client, id string) (*models.Job, error) {
	key := jobKeyPrefix + id
	data, err := rdb.HGetAll(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, config.ErrNil
	}

	job := &models.Job{
		ID:      data["id"],
		Type:    data["type"],
		Payload: data["payload"],
		Status:  data["status"],
		Result:  data["result"],
	}
	if data["retries"] != "" {
		if retries, err := strconv.Atoi(data["retries"]); err == nil {
			job.Retries = retries
		}
	}

	return job, nil
}

func UpdateStatus(ctx context.Context, rdb *config.Client, id string, status string, result string) error {
	key := jobKeyPrefix + id
	fields := map[string]string{
		"status": status,
		"result": result,
	}
	return rdb.HSet(ctx, key, fields)
}

func IncrementRetry(ctx context.Context, rdb *config.Client, job *models.Job) (int, error) {
	job.Retries++
	key := jobKeyPrefix + job.ID
	if err := rdb.HSet(ctx, key, map[string]string{"retries": strconv.Itoa(job.Retries)}); err != nil {
		return job.Retries, err
	}
	return job.Retries, nil
}

func MoveToFailedQueue(ctx context.Context, rdb *config.Client, job *models.Job) error {
	job.Status = models.StatusFailed
	if err := saveJob(ctx, rdb, job); err != nil {
		return err
	}
	return queue.MoveToFailed(ctx, rdb, job)
}

func RequeueJob(ctx context.Context, rdb *config.Client, job *models.Job) error {
	job.Status = models.StatusRetrying
	if err := saveJob(ctx, rdb, job); err != nil {
		return err
	}
	return queue.Requeue(ctx, rdb, job)
}

func saveJob(ctx context.Context, rdb *config.Client, job *models.Job) error {
	key := jobKeyPrefix + job.ID
	fields := map[string]string{
		"id":      job.ID,
		"type":    job.Type,
		"payload": job.Payload,
		"status":  job.Status,
		"retries": strconv.Itoa(job.Retries),
		"result":  job.Result,
	}
	return rdb.HSet(ctx, key, fields)
}

func MaxRetries() int {
	return maxRetries()
}

func newJobID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(nowUnixNano(), 10)
	}
	return hex.EncodeToString(buf)
}

var nowUnixNano = func() int64 {
	return time.Now().UnixNano()
}
