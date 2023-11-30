package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type Database struct {
	Filepath string
	Tables   map[string]*Table
	mutex    sync.RWMutex
}

type Table struct {
	Name      string
	Data      map[string][]interface{}
	RowsCount int
	labelTx   map[int]bool // индексы строк, которые еще не были закомичены транзакцией
	mutex     sync.RWMutex
}

func NewDatabase(filepath string) *Database {
	return &Database{
		Filepath: filepath,
		Tables:   make(map[string]*Table),
		mutex:    sync.RWMutex{},
	}
}

func LoadDB(filename string) (*Database, error) {

	db := NewDatabase(filename)

	jsonDB, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(jsonDB, db)
	if err != nil {
		return nil, err
	}

	for _, table := range db.Tables {
		table.labelTx = make(map[int]bool)
		table.mutex = sync.RWMutex{}
	}

	return db, nil
}

func (d *Database) SaveBD(ctx context.Context) error {
	d.mutex.Lock()
	d.MutexLock()
	defer d.mutex.Unlock()
	defer d.MutexUnLock()

	data, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}
	err = os.WriteFile(d.Filepath, data, 0644)
	if err != nil {
		return err
	}
	return nil

}

func (d *Database) MutexLock() {
	for _, table := range d.Tables {
		table.mutex.Lock()
	}
}

func (d *Database) MutexUnLock() {
	for _, table := range d.Tables {
		table.mutex.Unlock()
	}
}

func (d *Database) Begin(ctx context.Context) (pgx.Tx, error) {
	return &Dbtx{conn: d}, nil
}

func (d *Database) Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, insertedRows map[string][]int, err error) {
	if strings.HasPrefix(sql, "INSERT INTO") {
		insertedRows := make(map[string][]int)
		// sql = INSERT INTO articles id,name,rating VALUES ?,?,?
		//INSERT INTO articles id,name,rating VALUES .....
		// 0      1       2      3        4     5
		sqlParse := strings.Fields(sql)
		tableName := sqlParse[2]
		fields := strings.Split(sqlParse[3], ",")

		d.mutex.RLock()
		requireTable, ok := d.Tables[tableName]
		d.mutex.RUnlock()
		if !ok {
			return pgconn.CommandTag{}, insertedRows, errors.New("exec: table with this name not found")
		}

		requireTable.mutex.Lock()
		defer requireTable.mutex.Unlock()
		//мьютекс блокируется пока новая строка не будет полностью добавлена
		//чтобы другая транзакция не пыталась записать на её место другую строку

		for idx, fieldName := range fields {
			relevantArg := arguments[idx]
			requireTable.Data[fieldName] = append(requireTable.Data[fieldName], relevantArg)
		}

		for fieldName, listOfValues := range requireTable.Data {
			if len(listOfValues) < requireTable.RowsCount {
				requireTable.Data[fieldName] = append(requireTable.Data[fieldName], nil)
			}
		}

		//добавляем метку транзакции
		requireTable.labelTx[requireTable.RowsCount] = true

		insertedRows[tableName] = make([]int, 0)
		insertedRows[tableName] = append(insertedRows[tableName], requireTable.RowsCount)

		requireTable.RowsCount++

		return pgconn.CommandTag([]byte(`INSERT 1`)), insertedRows, nil
	}
	return pgconn.CommandTag{}, map[string][]int{}, errors.New("incorrect sql")
}

func (d *Database) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows := Rows{value: make([]map[string]interface{}, 0)}

	if !strings.HasPrefix(sql, "SELECT") {
		return rows, errors.New("incorrect sql")
	}

	// SELECT * FROM table WHERE id = ?
	//   0    1   2   3      4    5

	sqlParse := strings.Fields(sql)
	tableName := sqlParse[3]
	whereField := sqlParse[5]

	d.mutex.RLock()
	requireTable, ok := d.Tables[tableName]
	d.mutex.RUnlock()
	if !ok {
		return Rows{}, errors.New("table with this name not found")
	}

	relevantArg := args[0]

	needRows := make([]int, 0) // индексы строк, которые нам нужны

	requireTable.mutex.RLock()
	defer requireTable.mutex.RUnlock()

	listOfValues := requireTable.Data[whereField]
	for idx, value := range listOfValues {

		valueInt, valueIsInt := value.(float64) // float64 - потому json хранит в этом типе числа
		argInt, argIsInt := relevantArg.(float64)
		if valueIsInt && argIsInt {
			if valueInt == argInt && !requireTable.labelTx[idx] { // не отбираем незакомиченные строки
				needRows = append(needRows, idx)
				continue
			}
		}

		valueStr, valueIsString := value.(string)
		argStr, argIsString := relevantArg.(string)
		if valueIsString && argIsString {
			if valueStr == argStr && !requireTable.labelTx[idx] {
				needRows = append(needRows, idx)
				continue
			}
		}
	}

	for _, idx := range needRows {
		rowsMap := make(map[string]interface{})
		for field, values := range requireTable.Data {
			rowsMap[field] = values[idx]
		}
		rows.value = append(rows.value, rowsMap)
	}

	return rows, nil
}
