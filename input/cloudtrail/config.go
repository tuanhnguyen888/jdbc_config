package cloudtrail

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"time"
)

type config struct {
	APITimeout          time.Duration        `config:"api_timeout"`
	VisibilityTimeout   time.Duration        `config:"visibility_timeout"`
	SQSWaitTime         time.Duration        `config:"sqs.wait_time"`
	SQSMaxReceiveCount  int                  `config:"sqs.max_receive_count"`
	//SQSScript           *scriptConfig        `config:"sqs.notification_parsing_script"`
	FIPSEnabled         bool                 `config:"fips_enabled"`
	MaxNumberOfMessages int                  `config:"max_number_of_messages"`
	QueueURL            string               `config:"queue_url"`
	BucketARN           string               `config:"bucket_arn"`
	BucketListInterval  time.Duration        `config:"bucket_list_interval"`
	BucketListPrefix    string               `config:"bucket_list_prefix"`
	NumberOfWorkers     int                  `config:"number_of_workers"`
	AWSConfig           ConfigAWS  `config:",inline"`
	//FileSelectors       []fileSelectorConfig `config:"file_selectors"`
	//ReaderConfig        readerConfig         `config:",inline"`
}

func defaultConfig() config {
	c := config{
		APITimeout:          120 * time.Second,
		VisibilityTimeout:   300 * time.Second,
		BucketListInterval:  120 * time.Second,
		BucketListPrefix:    "",
		SQSWaitTime:         20 * time.Second,
		SQSMaxReceiveCount:  5,
		FIPSEnabled:         false,
		MaxNumberOfMessages: 5,
	}
	//c.ReaderConfig.InitDefaults()
	return c
}

func (c *config) Validate() error {
	if c.QueueURL == "" && c.BucketARN == "" {
		logrus.Warnf("Neither queue_url nor bucket_arn were provided, input will stop. ")
		return nil
	}

	//if c.QueueURL != "" && c.BucketARN != "" {
	//	return fmt.Errorf("queue_url <%v> and bucket_arn <%v> "+
	//		"cannot be set at the same time", c.QueueURL, c.BucketARN)
	//}

	//if c.BucketARN != "" && c.BucketListInterval <= 0 {
	//	return fmt.Errorf("bucket_list_interval <%v> must be greater than 0", c.BucketListInterval)
	//}
	//
	//if c.BucketARN != "" && c.NumberOfWorkers <= 0 {
	//	return fmt.Errorf("number_of_workers <%v> must be greater than 0", c.NumberOfWorkers)
	//}

	if c.QueueURL != "" && (c.VisibilityTimeout <= 0 || c.VisibilityTimeout.Hours() > 12) {
		return fmt.Errorf("visibility_timeout <%v> must be greater than 0 and "+
			"less than or equal to 12h", c.VisibilityTimeout)
	}

	if c.QueueURL != "" && (c.SQSWaitTime <= 0 || c.SQSWaitTime.Seconds() > 20) {
		return fmt.Errorf("wait_time <%v> must be greater than 0 and "+
			"less than or equal to 20s", c.SQSWaitTime)
	}

	if c.QueueURL != "" && c.MaxNumberOfMessages <= 0 {
		return fmt.Errorf("max_number_of_messages <%v> must be greater than 0",
			c.MaxNumberOfMessages)
	}

	if c.QueueURL != "" && c.APITimeout < c.SQSWaitTime {
		return fmt.Errorf("api_timeout <%v> must be greater than the sqs.wait_time <%v",
			c.APITimeout, c.SQSWaitTime)
	}

	return nil
}