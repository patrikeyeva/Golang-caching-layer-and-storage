package pkg

import (
	"encoding/json"
	"log"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
)

type Rows struct {
	value []map[string]interface{}
}

func (r Rows) Close() {}
func (r Rows) Err() error {
	return nil
}
func (r Rows) CommandTag() pgconn.CommandTag {
	jsonRows := make([]byte, 0)
	for _, row := range r.value {
		jsonRow, err := json.Marshal(row)
		if err != nil {
			log.Println(err)
			return pgconn.CommandTag{}
		}
		jsonRows = append(jsonRows, jsonRow...)
	}

	return pgconn.CommandTag(jsonRows)
}
func (r Rows) FieldDescriptions() []pgproto3.FieldDescription {
	return []pgproto3.FieldDescription{}
}
func (r Rows) Next() bool {
	return false
}
func (r Rows) Scan(dest ...interface{}) error {
	return nil
}
func (r Rows) Values() ([]interface{}, error) {

	return nil, nil
}
func (r Rows) RawValues() [][]byte {
	return nil
}
