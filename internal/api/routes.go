package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/Gargi2305/Distributed_job_processor/internal/config"
	"github.com/Gargi2305/Distributed_job_processor/internal/handlers"
	"github.com/Gargi2305/Distributed_job_processor/internal/service"
)

type Server struct {
	Redis *config.Client
}

type createJobRequest struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

func NewServer(rdb *config.Client) *Server {
	return &Server{Redis: rdb}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", s.handleJobs)
	mux.HandleFunc("/jobs/", s.handleJobByID)
	return mux
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.handleCreateJob(w, r)
}

func (s *Server) handleJobByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.handleGetJob(w, r)
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if req.Type == "" {
		http.Error(w, "type is required", http.StatusBadRequest)
		return
	}
	if !handlers.Supports(req.Type) {
		http.Error(w, "unsupported job type", http.StatusBadRequest)
		return
	}

	job, err := service.CreateJob(r.Context(), s.Redis, req.Type, req.Payload)
	if err != nil {
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(job)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" || strings.Contains(id, "/") {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	job, err := service.GetJob(r.Context(), s.Redis, id)
	if err != nil {
		if err == config.ErrNil {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to fetch job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(job)
}
