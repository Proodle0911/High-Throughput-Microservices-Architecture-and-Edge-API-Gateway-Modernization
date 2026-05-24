package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type IdempotencyRepo interface {
	SetIfNotExist(ctx context.Context, key string, expiration time.Duration) (bool, error)
}

type redisIdempotencyRepo struct {
	client *redis.Client
}

func NewRedisIdempotencyRepo(client *redis.Client) IdempotencyRepo {
	return &redisIdempotencyRepo{client: client}
}

func (r *redisIdempotencyRepo) SetIfNotExist(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	// key format: idempotency:{key}
	redisKey := fmt.Sprintf("idempotency:%s", key)
	return r.client.SetNX(ctx, redisKey, "locked", expiration).Result()
}
