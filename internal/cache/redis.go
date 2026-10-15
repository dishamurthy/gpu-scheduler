package cache

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb *redis.Client
}

func NewClient(url string) (*Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Client{rdb: rdb}, nil
}

func (c *Client) Close() error { return c.rdb.Close() }

func (c *Client) SetAlgorithm(ctx context.Context, algo string) error {
	return c.rdb.Set(ctx, "scheduler:algorithm", algo, 0).Err()
}

func (c *Client) GetAlgorithm(ctx context.Context) (string, error) {
	val, err := c.rdb.Get(ctx, "scheduler:algorithm").Result()
	if err == redis.Nil {
		return "FIFO", nil
	}
	return val, err
}

func (c *Client) IncrPreemptions(ctx context.Context) {
	c.rdb.Incr(ctx, "scheduler:preemptions")
}

func (c *Client) GetPreemptions(ctx context.Context) (int64, error) {
	val, err := c.rdb.Get(ctx, "scheduler:preemptions").Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}
