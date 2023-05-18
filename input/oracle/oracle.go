package oracle

import (
	"database/sql"
	"github.com/sirupsen/logrus"
)

type IDB interface {
	Close() error
	Query(statement string) (IRow,error)
}

type IRow interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest []interface{}) ([]interface{}, error)
	Close() error
}

type oracleImpl struct {
	db *sql.DB
}

type rowsImpl struct {
	rows *sql.Rows
}

func (o *oracleImpl) Close() error  {
	 return  o.db.Close()
}

func (o *oracleImpl) Query(statement string) (IRow,error) {
 	rows,err := o.db.Query(statement)
 	rowsImpl := &rowsImpl{rows: rows}
	return rowsImpl,err
}

func (r *rowsImpl) Columns() ([]string, error)  {
	return r.rows.Columns()
}

func (r *rowsImpl) Next() bool  {
	return r.rows.Next()
}

func (r *rowsImpl) Scan(dest []interface{}) ([]interface{}, error)  {
	values := make([]interface{},len(dest))
	for i := 0; i < len(dest); i++ {
		dest[i] = &values[i]
	}
	err :=  r.rows.Scan(dest...)
	return values, err
}

func (r *rowsImpl) Close() error {
	return r.rows.Close()
}

func ScanValue (rows IRow) ([]map[string]interface{}, error) {
	a := []map[string]interface{}{}
	b := map[string]interface{}{}
	columns , _ := rows.Columns()
	logrus.Info(columns)
	values := make([]interface{},len(columns))
	valuesPtr := make([]interface{},len(columns))
	for i := range values{
		valuesPtr[i] = &values[i]
	}
	defer rows.Close()
	for rows.Next(){
		values, err := rows.Scan(valuesPtr)
		if err != nil {
			return nil,err
		}
		for i, col := range columns{
			val := values[i]
			b[col] = val
		}

		a = append(a,b)
	}
	return a,nil
}