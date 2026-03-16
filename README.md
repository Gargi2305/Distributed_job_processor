# Distributed Job Processing System

Distributed job processing system in Go using Redis. Clients submit jobs through an HTTP API, the API pushes work onto a Redis list, and worker nodes process jobs asynchronously. The code uses only the Go standard library so it can build in restricted environments too.

## Architecture

Client -> API Server -> Redis Queue -> Worker Pool -> Job Handler -> Result Storage

- API server accepts jobs and stores metadata
- Redis `LIST` acts as the queue using `LPUSH` and `BRPOP`
- Worker pool pulls jobs concurrently and executes handlers
- Job metadata is stored in Redis hashes under `job:{id}`
- Failed jobs are copied into the `failed_jobs` queue

## Folder Structure

Matches the requested project layout.

## Environment Variables

- `REDIS_ADDR` (default `localhost:6379`)
- `WORKER_COUNT` (default `5`)
- `MAX_RETRIES` (default `3`)
- `PORT` (default `8080`)

## Run with Docker

```bash
docker-compose -f docker/docker-compose.yml up
```

Scale workers:

```bash
docker-compose -f docker/docker-compose.yml up --scale worker=3
```

## API Usage

Submit a job:

```bash
curl -X POST localhost:8080/jobs -H 'Content-Type: application/json' -d '{"type":"email","payload":"hello"}'
```

Check status:

```bash
curl localhost:8080/jobs/{id}
```

## Job Types

- `email` (simulates 2s)
- `report` (simulates 5s)

## Job Statuses

- `queued`
- `processing`
- `completed`
- `failed`
- `retrying`
