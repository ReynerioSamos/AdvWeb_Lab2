package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// starts off the report generation
// This does ALL report execution at the start of the program execution to prevent
// every POST request from its work link to the workers with their own polls. This allows
// the locking behavior to be more robust and job execution to be asynchronous despite there being only
// one routine to assign workers to jobs
func (app *application) startReportWorker(ctx context.Context) {

	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		ticker := time.NewTicker(app.config.workerPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				app.logger.Info("report worker stopped")
				return
			case <-ticker.C:
				err := app.processNextReportJob(ctx)
				if err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, context.Canceled) {
					app.logger.Error("report worker failed", "error", err)
				}
			}
		}
	}()
}

// handles one job-processing attempt (while jobs can be done asynchronously, it can't be requested Asynchronously)
// claim a queued job -> apply delay -> generate report -> record outcome
// Also returns only one job result at a time
func (app *application) processNextReportJob(ctx context.Context) error {
	// Claimnext choses oldest job in Job Table (Created_At) to preserve FIFO (First in first out)
	job, err := app.models.Jobs.ClaimNext(ctx)
	if err != nil {
		return err
	}
	app.logger.Info("report job started", "job_id", job.PublicID,
		"artificial_delay", app.config.reportDelay)

	// The exact same simulated work now belongs to the worker, not the POST.
	if app.config.reportDelay > 0 {
		// where the artificial delay now exists
		// timer must expire before work is marked as "done" even if it was dene before
		timer := time.NewTimer(app.config.reportDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	// The actual report generation done by the worker
	// runs an aggregate query to the DB and return results to the API
	report, err := app.models.Reports.Generate(job.ConsumerID, job.Payload.From, job.Payload.To)
	if err != nil {
		// marks as failed if no job is returned
		return app.models.Jobs.MarkFailed(ctx, job.ID, err.Error())
	}
	// else use marshal to serialize the result.
	// is used as jobs inserted as JSONBs for faster processing in DBs
	// this just turns it back into a regular JSON to be read easier
	// in the GET /v1/jobs/{id} poll request in async.sh
	result, err := json.Marshal(report)
	if err != nil {
		return app.models.Jobs.MarkFailed(ctx, job.ID, err.Error())
	}
	if err := app.models.Jobs.MarkCompleted(ctx, job.ID, result); err != nil {
		return err
	}
	// returns if job is completed with its id and DB public ID
	app.logger.Info("report job completed", "job_id", job.PublicID)
	return nil
}
