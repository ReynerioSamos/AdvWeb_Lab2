package data

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/lib/pq"
)

// The report payload which holds inputs worker needs to run the report QUERY (not the report itself)
// Stroed as a JSON wih from and to objects to track who sent the request and who should receive the response
type ReportPayload struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// The actual Job payload, used as a record of the job details (also not the actual report)
// This Job struct outlives the original HTTP request
// Once 202 Accepted is sent, this job struct is used to audit if the work was done and what happened/ the result
type Job struct {
	// ConsumerID, JobType, Status, Paload represent the actual job, who it's linked too
	// its progress and what was sent with it to make the report on. Error Message, StartedAt, CompletedAt, CreatedAt
	// is more used to audit the jobs themselves to see if they were completed, how long it took and if they didnt a message from the
	// DB saying why it didnt.

	ID           string          `json:"-"`
	PublicID     string          `json:"id"`
	ConsumerID   string          `json:"consumer_id"`
	JobType      string          `json:"job_type"`
	Status       string          `json:"status"`
	Payload      ReportPayload   `json:"payload"`
	Result       json.RawMessage `json:"result,omitempty"`
	ErrorMessage *string         `json:"error_message,omitempty"`
	StartedAt    *time.Time      `json:"started_at,omitempty"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

type JobModel struct {
	DB *sql.DB
}

// Creates a new job in a queued state and sends back a sort of receipt from the DB (ID, PublicID, Status, Created At)
// The request is then passed on to the next goroutine (in this case back to the report handler).
// This is done asynchronously to prevent blocking on the report generation itself, as once this is done
// a new Job can be inserted

func (m JobModel) Insert(job *Job) error {
	//
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return err
	}
	// inserts into jobs table the consumer id, type of job and the job's payload (what thereport is actually done on)
	query := `INSERT INTO jobs (consumer_id, job_type, payload)
		VALUES ($1, $2, $3) RETURNING id, public_id, status, created_at`
	// 3 sec Timeout incase the database is overwhelmed or halted. This prevents blocking from a single insertion
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// queries and returns back the recipt mentioned earlier to track the job and it's progress
	err = m.DB.QueryRowContext(ctx, query, job.ConsumerID, job.JobType, payload).Scan(
		&job.ID, &job.PublicID, &job.Status, &job.CreatedAt,
	)
	if err != nil {
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return ErrRecordNotFound
		}
		return err
	}
	return nil
}

// What is mostly in the polling to return the status is of the job (in GET /v1/jobs/{id} call)
// Returns the current state of the DB entry in jobs at the moment of the poll.
// The polling (result) is actually in async.sh but this can be used in other polling implementations
// like a server side one of progress bar to provide feedback to a user
func (m JobModel) GetByPublicID(publicID string) (*Job, error) {
	// using COALESCE so when results are null (such as before a job is complete/incomplete) it can
	// still return the incomplete the JSON literal to keep the scan well-defined
	query := `SELECT id, public_id, consumer_id, job_type, status, payload,
		COALESCE(result, 'null'::jsonb), error_message, started_at, completed_at, created_at
		FROM jobs WHERE public_id = $1`
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var job Job
	var payload []byte
	err := m.DB.QueryRowContext(ctx, query, publicID).Scan(&job.ID, &job.PublicID,
		&job.ConsumerID, &job.JobType, &job.Status, &payload, &job.Result,
		&job.ErrorMessage, &job.StartedAt, &job.CompletedAt, &job.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrRecordNotFound
		}
		return nil, err
	}
	if err := json.Unmarshal(payload, &job.Payload); err != nil {
		return nil, err
	}
	return &job, nil
}

// Is what's mostly used for concurrent job processing.
// It allows workers to poll jobs from the jobs table while prevent workers from doing the same job
//
// Transactions are run coupled together by choosing a job -> marking as processing
// If it didnt, two workers could pick up the same job before it was makred as processing
// which is not ideal.
func (m JobModel) ClaimNext(ctx context.Context) (*Job, error) {
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	// Used to rollback job assignments incase an error occurs.
	// done to undo any partial work and allows other workers to pick up previously
	// unfinsihed state job (not incomplete) for themselves
	defer tx.Rollback()
	//ORDER BY used to make the job queuing FIFO (first in first out)
	//FOR UPDATE locks the selected job row for the transaction
	//SKIP LOCKED is what does the blocking to prevent other workers from taking job if
	// one is already working on it
	//LIMIT 1 ensures call claims only ONE job, meaning ONE WORKER works on ONE JOB
	// this could later be used to allow multiple jobs to a single worker
	query := `SELECT id, public_id, consumer_id, job_type, payload FROM jobs
		WHERE status = 'queued' AND job_type = 'consumer_activity_report'
		ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1`
	var job Job
	var payload []byte
	if err := tx.QueryRowContext(ctx, query).Scan(&job.ID, &job.PublicID,
		&job.ConsumerID, &job.JobType, &payload); err != nil {
		return nil, err
		
	}
	if err := json.Unmarshal(payload, &job.Payload); err != nil {
		return nil, err
	}
	// marks the row as processing inside same transaction, prevent two workers from getting the same job
	if _, err := tx.ExecContext(ctx,
		`UPDATE jobs SET status = 'processing', started_at = now() WHERE id = $1`, job.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	// change of the job is reflected to the caller to see the status, used for http status and polling
	job.Status = "processing"
	return &job, nil
}

// simply makrs a job as complete when it returns an expected result (complete/incomplete)
func (m JobModel) MarkCompleted(ctx context.Context, id string, result []byte) error {
	_, err := m.DB.ExecContext(ctx,
		`UPDATE jobs SET status = 'completed', result = $2, completed_at = now() WHERE id = $1`,
		id, result)
	return err
}

// marks the job as failed if an unexpected result is sent back
func (m JobModel) MarkFailed(ctx context.Context, id, message string) error {
	_, err := m.DB.ExecContext(ctx,
		`UPDATE jobs SET status = 'failed', error_message = $2, completed_at = now() WHERE id = $1`,
		id, message)
	return err
}
