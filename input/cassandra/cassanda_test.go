package cassandra

import (
	"github.com/sirupsen/logrus"
	"testing"
)

func TestGetKeyspaceFromQuery(t *testing.T) {
	sql := "SELECT * FROM mykeyspace.mytable WHERE column = 'value'"
	keyspace :=	GetKeyspaceFromQuery(sql)
	logrus.Info(keyspace)
}