package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/Gargi2305/Distributed_job_processor/internal/api"
	"github.com/Gargi2305/Distributed_job_processor/internal/config"
	"github.com/Gargi2305/Distributed_job_processor/pkg/logger"
)

func main() {
	rdb := config.NewRedisClient()
	server := api.NewServer(rdb)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	addr := ":" + port
	logger.Info("api_started", fmt.Sprintf("addr=%s", addr))
	if err := http.ListenAndServe(addr, server.Routes()); err != nil {
		logger.Error("api_failed", err.Error())
		os.Exit(1)
	}
}
