// TODO Вы можете редактировать этот файл по вашему усмотрению

package database

import (
	"context"
	"encoding/json"
	"errors"
	"homework-7/pkg"
	"log"
	"regexp"
	"strconv"
	"time"
)

const sqlInsert = "INSERT INTO articles id,name,rating VALUES ?,?,?"
const sqlSelect = "SELECT * FROM articles WHERE id = ?"

type Client struct {
	db *pkg.Database
}

type Article struct {
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	Rating int64  `json:"rating"`
}

func NewClient(db *pkg.Database) *Client {
	return &Client{db: db}
}

func (c *Client) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	log.Println("database INSERT start")
	defer log.Println("database INSERT end")

	tx, err := c.db.Begin(ctx)
	if err != nil {
		log.Println(err)
		return err
	}

	if jsonStr, ok := value.(string); ok {
		var article Article
		err = json.Unmarshal([]byte(jsonStr), &article)
		if err != nil {
			log.Println(err)
			return err
		}
		result, err := tx.Exec(ctx, sqlInsert, article.ID, article.Name, article.Rating)
		if err != nil || !result.Insert() {
			tx.Rollback(ctx)
			log.Println(err)
			return err
		}
		log.Println(result.String())
		if err = tx.Commit(ctx); err != nil {
			log.Println(err)
			return err
		}
		return nil
	}
	return errors.New("wrong value type")
}

func (c *Client) Get(ctx context.Context, key string) (any, error) {
	log.Println("database SELECT start")
	defer log.Println("database SELECT end")

	db, err := pkg.LoadDB(c.db.Filepath)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	re, err := regexp.Compile(`id:(\d+)`)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	var id float64
	matches := re.FindStringSubmatch(key)
	if len(matches) >= 2 {
		id, err = strconv.ParseFloat(matches[1], 64)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	} else {
		return nil, errors.New("no id find")
	}

	rows, err := db.Query(ctx, sqlSelect, id)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	result := rows.CommandTag()

	return result.String(), nil

}
