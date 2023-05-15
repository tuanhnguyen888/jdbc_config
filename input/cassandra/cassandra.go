package cassandra

import "strings"

func GetKeyspaceFromQuery(query string) string {
	// Tìm vị trí của từ khóa "FROM" trong câu truy vấn
	fromIndex := strings.Index(strings.ToUpper(query), "FROM")
	if fromIndex == -1 {
		return ""
	}

	substring := query[fromIndex+len("FROM"):]

	// Xóa các khoảng trắng và ký tự phụ đầu câu
	substring = strings.TrimSpace(substring)
	// Tìm vị trí của khoảng trắng hoặc dấu chấm đầu tiên
	spaceIndex := strings.IndexAny(substring, " .")
	if spaceIndex == -1 {
		return substring
	}
	keyspace := substring[:spaceIndex]

	return keyspace
}