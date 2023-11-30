package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache struct {
	data       map[string]string
	expiration map[string]time.Time
	mutex      sync.RWMutex
}

func NewCache(ctx context.Context) *Cache {
	cache := &Cache{
		data:       make(map[string]string),
		expiration: make(map[string]time.Time),
		mutex:      sync.RWMutex{},
	}
	go cache.BackgroundCleaning(ctx)
	return cache
}

func (c *Cache) Get(ctx context.Context, key string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, "get", key)

	c.mutex.RLock()
	value, exists := c.data[key]
	c.mutex.RUnlock()
	if !exists {
		cmd.SetErr(errors.New("key doesn't exist in cache"))
		return cmd
	}

	c.mutex.RLock()
	expiration := c.expiration[key]
	c.mutex.RUnlock()
	if time.Now().After(expiration) {
		deleteFromCache(c, key)
		cmd.SetErr(errors.New("key expired"))
		return cmd
	}

	cmd.SetErr(nil)
	cmd.SetVal(value)

	return cmd
}

func (c *Cache) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {

	// создаем объект типа *redis.StatusCmd
	minLenOfArgs, maxLenOfArgs := 3, 5
	args := make([]interface{}, minLenOfArgs, maxLenOfArgs)
	args[0] = "set"
	args[1] = key
	args[2] = value
	if expiration > 0 {
		args = append(args, "ex", expiration)
	}
	cmd := redis.NewStatusCmd(ctx, args...)

	byteValue, err := json.Marshal(value)
	if err != nil {
		cmd.SetErr(errors.New("can't marshal value to json"))
		return cmd
	}

	c.mutex.Lock()
	c.data[key] = string(byteValue)
	if expiration > 0 {
		c.expiration[key] = time.Now().Add(expiration)
	}
	c.mutex.Unlock()

	cmd.SetErr(nil)
	cmd.SetVal("OK")

	return cmd
}

func (c *Cache) BackgroundCleaning(ctx context.Context) {
	defer log.Println("Complete cache background cleaning")
	for {
		time.Sleep(1 * time.Minute)
		select {
		case <-ctx.Done():
			return

		default:
			now := time.Now()
			c.mutex.RLock()
			for key, expTime := range c.expiration {
				c.mutex.RUnlock()
				if now.After(expTime) {
					deleteFromCache(c, key)
				}
				c.mutex.RLock()
			}
			c.mutex.RUnlock()
		}
	}
}

func deleteFromCache(c *Cache, key string) {
	c.mutex.Lock()
	delete(c.data, key)
	delete(c.expiration, key)
	c.mutex.Unlock()
}
