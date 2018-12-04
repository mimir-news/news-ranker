package main

import (
	"log"
	"sync"

	"github.com/mimir-news/pkg/mq"
)

type handlerFunc func(msg mq.Message) error

type handler struct {
	queue  string
	client mq.Client
	fn     handlerFunc
}

func newHandler(queue string, client mq.Client, fn handlerFunc) handler {
	return handler{
		queue:  queue,
		client: client,
		fn:     fn,
	}
}

func handleSubscription(h handler, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	messageChannel, err := h.client.Subscribe(h.queue, ServiceName)
	if err != nil {
		log.Println(err)
		return
	}

	for msg := range messageChannel {
		err = h.fn(msg)
		wrapMessageHandlingResult(msg, err)
	}
}

func wrapMessageHandlingResult(msg mq.Message, err error) {
	if err != nil {
		log.Println(err)
		rejectErr := msg.Reject()
		if rejectErr != nil {
			log.Println(rejectErr)
		}
	} else {
		ackErr := msg.Ack()
		if ackErr != nil {
			log.Println(ackErr)
		}
	}
}
