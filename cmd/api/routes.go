package main

import "net/http"

// routes for the HTTP operations of the app

func (app *application) routes() http.Handler {
	mux := http.NewServeMux()
	// reports health of the server
	mux.HandleFunc("GET /v1/healthcheck", app.healthcheckHandler)
	// creates a new consumer in the consumer DB
	mux.HandleFunc("POST /v1/consumers", app.createConsumersHandler)
	// used for inital CREATION of the report with further job creation, worker assignment, work done
	mux.HandleFunc("POST /v1/reports", app.createReportHandler)
	// gets the current status of a JOB and its relevant information. This route is used for polling also
	mux.HandleFunc("GET /v1/jobs/{id}", app.getJobHandler)
	return mux
}
