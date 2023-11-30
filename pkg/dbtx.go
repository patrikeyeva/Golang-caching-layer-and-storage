package pkg

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type Dbtx struct {
	conn *Database

	// map[имя таблицы] массив индексов
	insertedRows map[string][]int // индексы строк, добавленных в рамках данной транзакции
}

func (tx *Dbtx) Commit(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		tx.DeleteLabels()
		return tx.conn.SaveBD(ctx)
	}
}
func (tx *Dbtx) Rollback(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		//удаляем метки транзакции
		tx.DeleteLabels()

		//удаляем строки из таблицы
		for tableName, table := range tx.insertedRows {
			for _, idx := range table {
				tx.conn.mutex.Lock()
				for _, values := range tx.conn.Tables[tableName].Data {
					removeAtIndex(values, idx)
				}
				tx.conn.mutex.Unlock()
			}
		}
		return nil
	}
}

func (tx *Dbtx) Exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error) {
	commandTag, insertedRows, err := tx.conn.Exec(ctx, sql, arguments...)
	if err == nil {
		tx.insertedRows = insertedRows
	}
	return commandTag, err

}
func (tx *Dbtx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return tx.conn.Query(ctx, sql, args...)
}

func (tx *Dbtx) DeleteLabels() {
	for tableName, table := range tx.insertedRows {
		requireTable := tx.conn.Tables[tableName]
		for _, idx := range table {
			requireTable.mutex.Lock()
			delete(requireTable.labelTx, idx)
			requireTable.mutex.Unlock()
		}
	}
}

func removeAtIndex(slice []interface{}, index int) []interface{} {
	return append(slice[:index], slice[index+1:]...)
}

/*---------------------------------------------------*/

func (tx *Dbtx) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return nil
}

func (tx *Dbtx) Begin(ctx context.Context) (pgx.Tx, error) {
	return nil, nil
}
func (tx *Dbtx) BeginFunc(ctx context.Context, f func(pgx.Tx) error) (err error) {
	return nil
}

func (tx *Dbtx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (tx *Dbtx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return nil
}
func (tx *Dbtx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}

}
func (tx *Dbtx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, nil

}

func (tx *Dbtx) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	return nil, nil

}
func (tx *Dbtx) Conn() *pgx.Conn {
	return nil
}
