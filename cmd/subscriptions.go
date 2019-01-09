package main

import (
	"fmt"
	"sync"

	"github.com/mimir-news/pkg/id"
	"github.com/mimir-news/pkg/mq"
)

type handlerFunc func(msg mq.Message, messageId string) error

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
	consumerID := newConsumerID()
	logger.Infow("Starting subscription", "queue", h.queue, "consumerId", consumerID)
	messageChannel, err := h.client.Subscribe(h.queue, consumerID)
	if err != nil {
		logger.Errorw("Channel subscription failed", "queue", h.queue, "error", err)
		return
	}

	for msg := range messageChannel {
		err = h.fn(msg, id.New())
		wrapMessageHandlingResult(msg, err, h.queue)
	}
}

func wrapMessageHandlingResult(msg mq.Message, err error, queueName string) {
	if err != nil {
		logger.Errorw("Channel subscription failed", "queue", queueName, "error", err)
		rejectErr := msg.Reject()
		if rejectErr != nil {
			logger.Errorw("Reject failed", "queue", queueName, "error", rejectErr)
		}
	} else {
		ackErr := msg.Ack()
		if ackErr != nil {
			logger.Errorw("Ack failed", "queue", queueName, "error", ackErr)
		}
	}
}

func newConsumerID() string {
	return fmt.Sprintf("%s-%s", ServiceName, id.New())
}
