package redisdb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

var subscribeService *serviceImpl

type Handler func(string) error

func Subscribe(handler Handler) error {
	if subscribeService == nil {
		return errors.New("unimplemented")
	}

	return subscribeService.subscribe(handler)
}

// serviceImpl
type serviceImpl struct {
	subscribers []*subscriber

	db       int
	redisCli *redis.Client
}

func (impl *serviceImpl) unsubscribe() {
	items := impl.subscribers
	for i := range items {
		items[i].exit()
	}
}

func (impl *serviceImpl) subscribe(handler Handler) error {
	s := subscriber{
		db:       impl.db,
		redisCli: impl.redisCli,
		done:     make(chan struct{}),
	}

	err := s.start(handler)
	if err == nil {
		impl.subscribers = append(impl.subscribers, &s)
	}

	return err
}

// subscriber
type subscriber struct {
	db       int
	redisCli *redis.Client

	once   sync.Once
	done   chan struct{}
	cancel context.CancelFunc
}

func (s *subscriber) exit() {
	s.once.Do(func() {
		s.cancel()

		// wait
		<-s.done
	})
}

func (s *subscriber) start(handler Handler) error {
	ctx := context.Background()

	_, err := s.redisCli.ConfigSet(ctx, "notify-keyspace-events", "Ex").Result()
	if err != nil {
		return err
	}

	ctx, s.cancel = context.WithCancel(ctx)

	go func(ctx context.Context) {
		defer close(s.done)

		pubsub := s.redisCli.Subscribe(ctx, fmt.Sprintf("__keyevent@%d__:expired", s.db))
		defer pubsub.Close()

		for {
			select {
			case msg := <-pubsub.Channel():
				if err = handler(msg.Payload); err != nil {
					logrus.Errorf("handle err: %s", err.Error())
				}

			case <-ctx.Done():
				logrus.Info("exit normally.")

				return
			}
		}
	}(ctx)

	return nil
}
