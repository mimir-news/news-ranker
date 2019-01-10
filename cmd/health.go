package main

import (
	"time"

	"github.com/CzarSimon/go-file-heartbeat/heartbeat"
	"github.com/mimir-news/pkg/dbutil"
	"github.com/mimir-news/pkg/id"
)

func (e *env) healthCheck() {
	for {
		sleep(e.config.HearbeatInterval)
		checkID := id.New()
		err := dbutil.IsConnected(e.db)
		if err != nil {
			logger.Errorw("health check failed", "reason", "DB not connected", "healthCheckId", checkID, "err", err)
			continue
		}

		if !e.mqClient.Connected() {
			logger.Errorw("health check failed", "reason", "MQ disconnected", "healthCheckId", checkID)
			continue
		}

		logger.Infow("health check OK emitting heartbeat", "healthCheckId", checkID)
		heartbeat.EmitToFile(e.config.HearbeatFile)
	}

}

func sleep(seconds int) {
	sleepSeconds := time.Duration(seconds) * time.Second
	time.Sleep(sleepSeconds)
}
