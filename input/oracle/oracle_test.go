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

	// Thực hiện câu lệnh SELECT
	query := "SELECT * FROM ACCESS$"
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Lấy thông tin về các cột trong kết quả truy vấn
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	// Tạo slice để lưu trữ các hàng dữ liệu
	data := make([]map[string]interface{}, 0)

	// Lấy và đưa ra dữ liệu từ các hàng
	for rows.Next() {
		// Tạo slice để lưu trữ các giá trị trong hàng
		values := make([]interface{}, len(columns))

		// Tạo slice để lưu trữ con trỏ đến các giá trị trong hàng
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// Scan các giá trị trong hàng vào slice values
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatal(err)
		}

		// Tạo map để lưu trữ các giá trị với key là tên cột
		rowData := make(map[string]interface{})
		for i, col := range columns {
			rowData[col] = values[i]
		}

		// Thêm map chứa dữ liệu của hàng vào slice data
		data = append(data, rowData)
	}

	// Kiểm tra lỗi trong quá trình duyệt hàng
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	// In ra dữ liệu
	for _, row := range data {
		for col, val := range row {
			fmt.Printf("%s: %v\n", col, val)
		}
		fmt.Println()
	}
}