package eventstore

import (
	"context"

	"github.com/redis/go-redis/v9"
)

func NewDefaultRedisClient(ctx context.Context, addr, username, password string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: username,
		Password: password,
		DB:       0, // use default DB
	})

	return client, nil
}
