package main

import (
	"log"
	"time"

	"github.com/CzarSimon/go-file-heartbeat/heartbeat"
	"github.com/mimir-news/pkg/dbutil"
)

func (e *env) healthCheck() {
	for {
		sleep(e.config.HearbeatInterval)
		err := dbutil.IsConnected(e.db)
		if err != nil {
			log.Println("ERROR -", err)
			continue
		}

		if !e.mqClient.Connected() {
			log.Println("ERROR - MQ disconnected")
			continue
		}

		heartbeat.EmitToFile(e.config.HearbeatFile)
	}

}

func sleep(seconds int) {
	sleepSeconds := time.Duration(seconds) * time.Second
	time.Sleep(sleepSeconds)
}
