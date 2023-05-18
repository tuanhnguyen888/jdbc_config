package oracle

import (
	"database/sql"
	"fmt"
	_ "gopkg.in/goracle.v2"
	"log"
	"testing"
)

func TestInit(t *testing.T) {
	// Tạo chuỗi kết nối đến cơ sở dữ liệu Oracle
	db, err := sql.Open("goracle", fmt.Sprintf("user=%s/%s@%s:%s/%s AS SYSDBA", "SYS", "1", "localhost", "1521", "XE"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Kiểm tra kết nối
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Kết nối thành công đến cơ sở dữ liệu Oracle XE")

	// Thực hiện truy vấn SELECT
	rows, err := db.Query("SELECT * FROM new_table")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Lấy danh sách các cột trong kết quả
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	// Tạo một slice để lưu trữ các dòng kết quả
	results := []map[string]interface{}{}

	// Lặp qua các dòng kết quả
	for rows.Next() {
		// Tạo một slice để lưu trữ các giá trị của dòng hiện tại
		values := make([]interface{}, len(columns))
		// Tạo một slice để lưu trữ các con trỏ đến các giá trị
		valuePointers := make([]interface{}, len(columns))
		for i := range values {
			valuePointers[i] = &values[i]
		}

		// Scan các giá trị vào con trỏ
		err := rows.Scan(valuePointers...)
		if err != nil {
			log.Fatal(err)
		}

		// Tạo một map để lưu trữ cặp key-value của dòng hiện tại
		rowData := make(map[string]interface{})
		for i, col := range columns {
			rowData[col] = values[i]
		}

		// Thêm map vào slice của kết quả
		results = append(results, rowData)
	}

	// Kiểm tra lỗi sau khi lặp qua các dòng
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	// In ra kết quả
	for _, row := range results {
		for col, val := range row {
			fmt.Printf("%s: %v\t", col, val)
		}
		fmt.Println()
	}
}