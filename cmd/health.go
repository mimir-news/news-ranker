package main

import (
	"time"

	"github.com/CzarSimon/go-file-heartbeat/heartbeat"
	"github.com/mimir-news/pkg/dbutil"
)

func (e *env) healthCheck() {
	for {
		sleep(e.config.HearbeatInterval)
		err := dbutil.IsConnected(e.db)
		if err != nil {
			logger.Errorw("health check failed", "reason", "DB not connected", "err", err)
			continue
		}

		if !e.mqClient.Connected() {
			logger.Errorw("health check failed", "reason", "MQ disconnected")
			continue
		}

		logger.Debug("health check OK")
		heartbeat.EmitToFile(e.config.HearbeatFile)
	}

}

func sleep(seconds int) {
	sleepSeconds := time.Duration(seconds) * time.Second
	time.Sleep(sleepSeconds)
}
