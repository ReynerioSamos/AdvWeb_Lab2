package main

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/lewisdalwin/gatekeeper/internal/data"
	"github.com/lewisdalwin/gatekeeper/internal/validator"
)

// the main HTTP handler for report handling
// decoupled from the actual work done from report generation now so it can
// handle a requests irregardless of if the work was done or not on the report
// as that's now offloaded to the worker.
// Returns the result of the HTTP request on if it was accepted or not, and handles another request
// if there is one immediately after
func (app *application) createReportHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ConsumerID string    `json:"consumer_id"`
		From       time.Time `json:"from"`
		To         time.Time `json:"to"`
	}
	if err := app.readJSON(w, r, &input); err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	// various validation is needed before a job can start being created
	// preventds malformed input
	v := validator.New()
	v.Check(input.ConsumerID != "", "consumer_id", "must be provided")
	v.Check(!input.From.IsZero(), "from", "must be provided")
	v.Check(!input.To.IsZero(), "to", "must be provided")
	v.Check(input.From.Before(input.To), "from", "must be earlier than to")
	if !v.Valid() {
		// return 400 status if malformed
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	// Job request output (indicating a job was created)
	// as soon as it returns, handler does not have a handle on it anymore
	job := &data.Job{
		ConsumerID: input.ConsumerID,
		JobType:    "consumer_activity_report",
		Payload:    data.ReportPayload{From: input.From, To: input.To},
	}
	if err := app.models.Jobs.Insert(job); err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
			return
		}
		app.serverErrorResponse(w, r, err)
		return
	}

	// job.PublicID in the DB is how a job is returned back here to the handler to audit a job's
	// outcome. This also allows a job to be returned even if the client losing connection to the server
	statusURL := fmt.Sprintf("/v1/jobs/%s", job.PublicID)
	headers := make(http.Header)
	headers.Set("Location", statusURL)
	response := envelope{"job_id": job.PublicID, "status": job.Status, "status_url": statusURL}
	if err := app.writeJSON(w, http.StatusAccepted, response, headers); err != nil {
		app.serverErrorResponse(w, r, err)
	}
}

// Returns the status/progress of the job using GET instead of createReportHandler's POST
// This is used in the polling and is used to preserve the state of the job as GET is a READ ONLY
// operation and cannot influence the job no matter how many times its called
func (app *application) getJobHandler(w http.ResponseWriter, r *http.Request) {
	job, err := app.models.Jobs.GetByPublicID(r.PathValue("id"))
	if err != nil {
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
			return
		}
		app.serverErrorResponse(w, r, err)
		return
	}
	if err := app.writeJSON(w, http.StatusOK, envelope{"job": job}, nil); err != nil {
		app.serverErrorResponse(w, r, err)
	}
}
