package metadata

import (
	"github.com/sirupsen/logrus"
	"time"
)

type Metadata struct {
	LastRun        string
	SqlLastValue   int64
	TrackingColumn string
}

func InitMetadata() {
	metadata := Metadata{
		LastRun:        time.Now().Format("Mon Jan _2 15:04:05 2006"),
		SqlLastValue:   1682827144000,
		TrackingColumn: "created_at",
	}

	logrus.Info(metadata)
}
