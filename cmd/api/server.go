package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// runs the HTTP server for both itself and workers
func (app *application) serve() error {
srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", app.config.port),
		Handler:      app.routes(),
		IdleTimeout:  time.Minute,
		ReadTimeout:  5 * time.Second,
		// write timeout is used to limit how long a HTTP response is allowed to write
		// used to drop connections that are hanging with no results
		// NOTE: this is ONLY for the HTTP response and not the actual report generation
		// so a delay of 12 secs can still be successfully completed as the http response is
		// only open once the ack of completion is sent and THEN the GET request is sent
		WriteTimeout: 10 * time.Second,
	}

	shutdownError := make(chan error)

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		s := <-quit

		app.logger.Info("caught signal", "signal", s.String())

		// gives 30 seconds for a HTTP request to finish before being forcefully closed
		// used for finishing remain jobs during shutdown proccess
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// stops new HTTP connections from being created and interrupts the inflight ones base on time
		err := srv.Shutdown(ctx)
		if err != nil {
			shutdownError <- err
			return 
		}

		app.logger.Info("completing background tasks", "addr", srv.Addr)


		if app.workerCancel != nil {
			app.workerCancel()
		}
		// blocks until worker goroutine is started
		app.wg.Wait()
		shutdownError <- nil
	}()

	app.logger.Info("starting server", "addr", srv.Addr, "env", app.config.env)

	err := srv.ListenAndServe()
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	err = <-shutdownError
	if err != nil {
		return err
	}

	app.logger.Info("stopped server", "addr", srv.Addr)

	return nil
}
