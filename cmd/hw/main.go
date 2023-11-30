package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"homework-7/internal/controller"
	"homework-7/internal/datasource/cache"
	"homework-7/internal/datasource/database"
	"homework-7/pkg"

	"github.com/joho/godotenv"
)

type Article struct {
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	Rating int64  `json:"rating"`
}

func main() {

	errEnv := godotenv.Load(".env")
	if errEnv != nil {
		panic(errEnv)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	db, errDB := pkg.LoadDB(os.Getenv("DATABASE_FILE"))
	if errDB != nil {
		panic(errDB)
	}
	client := controller.NewClient(cache.NewClient(pkg.NewCache(ctx), database.NewClient(db)))

	// Создаём запись
	value := `{"id":1,"name":"Cats","rating":10}`
	err := client.Set(ctx, "articles:id:1", value, 3*time.Second)
	if err != nil {
		panic(err)
	}

	// Получаем запись из кэша
	got, err := client.Get(ctx, "articles:id:1")
	if err != nil {
		panic(err)
	}

	fmt.Println(got)
	fmt.Println("Wait 5 sec")
	select {
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
	}
	// Получаем запись из базы данных и обновляем кэщ
	gotAgain, err := client.Get(ctx, "articles:id:1")

	if err != nil {
		panic("not string")
	}
	stringValue, ok := gotAgain.(string)
	if !ok {
		panic("err")
	}

	if stringValue != value {
		panic("invalid value")
	}

	var wg sync.WaitGroup

	fmt.Println(stringValue)

	log.Println("Begin the multithreading")
	for i := 2; i < 6; i++ {
		wg.Add(2)
		go func(id int) {
			defer wg.Done()

			article := &Article{
				ID:     int64(id),
				Name:   "Cats" + strconv.Itoa(id),
				Rating: int64(id + 10),
			}

			jsonBytes, errJ := json.Marshal(article)
			if errJ != nil {
				log.Println("Json error:", errJ)
			}
			value := string(jsonBytes)
			key := "articles:id:" + strconv.Itoa(id)
			err := client.Set(ctx, key, value, 3*time.Second)
			if err != nil {
				panic(err)
			}

		}(i)

		go func(id int) {
			defer wg.Done()

			key := "articles:id:" + strconv.Itoa(id)
			gotAgain, err := client.Get(ctx, key)

			if err != nil {
				panic("not string")
			}
			stringValue, ok := gotAgain.(string)
			if !ok {
				panic("err")
			}
			fmt.Println(stringValue)

		}(i)
	}

	wg.Wait()
}
