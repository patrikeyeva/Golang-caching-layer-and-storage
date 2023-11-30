// TODO Вы можете редактировать этот файл по вашему усмотрению

package cache

import (
	"context"
	"log"
	"time"

	"homework-7/internal/datasource"
	"homework-7/pkg"
)

type Client struct {
	cache  *pkg.Cache
	source datasource.Datasource
}

func NewClient(cache *pkg.Cache, sourse datasource.Datasource) *Client {
	return &Client{cache: cache, source: sourse}

}

func (c *Client) Set(ctx context.Context, key string, value any, expiration time.Duration) error {

	log.Println("cache Set start")
	defer log.Println("cache Set end")
	err := c.source.Set(ctx, key, value, expiration)
	if err != nil {
		return err
	}

	statusCmd := c.cache.Set(ctx, key, value, expiration)

	return statusCmd.Err()
}

func (c *Client) Get(ctx context.Context, key string) (any, error) {

	log.Println("cache Get start")
	defer log.Println("cache Get end")

	gotdata := c.cache.Get(ctx, key)
	if gotdata.Err() != nil { // notFound
		sourseData, err := c.source.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		statusCmd := c.cache.Set(ctx, key, sourseData, 10)

		if statusCmd.Err() != nil {
			return nil, statusCmd.Err()
		}
		return sourseData, nil

	}

	return gotdata.Val(), nil
}
