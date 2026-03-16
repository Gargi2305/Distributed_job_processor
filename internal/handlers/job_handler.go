package handlers

import (
	"fmt"
	"time"

	"github.com/Gargi2305/Distributed_job_processor/internal/models"
)

func Supports(jobType string) bool {
	switch jobType {
	case "email", "report":
		return true
	default:
		return false
	}
}

func Handle(job *models.Job) (string, error) {
	switch job.Type {
	case "email":
		time.Sleep(2 * time.Second)
		return fmt.Sprintf("email sent with payload=%s", job.Payload), nil
	case "report":
		time.Sleep(5 * time.Second)
		return fmt.Sprintf("report generated with payload=%s", job.Payload), nil
	default:
		return "", fmt.Errorf("unsupported job type: %s", job.Type)
	}
}
