package cloudtrail

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	ModuleName = "cloudtrail"
	sqsRetryDelay = 10 * time.Second
	sqsApproximateReceiveCountAttribute = "ApproximateReceiveCount"
)

// ConfigInput holds the configuration json fields and internal objects
type ConfigInput struct {
	//config.InputConfig
	QueueUrl		 		string				`mapstructure:"queue_url"`
	VisibilityTimeout 		int64				`mapstructure:"visibility_timeout"`
	SQSWaitTime         	int64       		`mapstructure:"sqs.wait_time"`
	MaxNumberOfMessages 	int					`mapstructure:"max_number_of_messages"`
	ApiTimeout 				int64				`mapstructure:"api_timeout"`
	SQSMaxReceiveCount  int                  	`mapstructure:"sqs.max_receive_count"`
	AccessKeyId				string				`mapstructure:"access_key_id"`
	SecretAccessKey			string				`mapstructure:"secret_access_key"`
	SessionToken 			string				`mapstructure:"session_token"`
	RoleArn				 	string				`mapstructure:"role_arn"`
	Endpoint          		string				`mapstructure:"endpoint"`
	DefaultRegion			string				`mapstructure:"default_region"`
	ProxyUrl		 		string				`mapstructure:"proxy_url"`
	sqsClient				*sqs.Client
	s3Client    			*s3.Client

}

// DefaultInputConfig returns an ConfigInput struct with default values
func DefaultInputConfig() ConfigInput {
	//[commonConfig.Name](http://commonconfig.name/) = ModuleName
	return ConfigInput{
		ApiTimeout:          120 ,
		VisibilityTimeout:   300 ,
		SQSWaitTime:         20 ,
		MaxNumberOfMessages: 5,
		Endpoint: "amazonaws.com",
		DefaultRegion: "us-east-1",

	}
}

//func InitHandler(ctx context.Context) (config.InputPlugin, error) {
//	conf := DefaultInputConfig(commonConfig)
//	if err := mapstructure.Decode(raw, &conf); err != nil {
//		return nil, err
//	}
//
//
//	if err := conf.Validate() ; err != nil {
//		return nil, err
//	}
//
//	codec := raw["codec"]
//	if codec == nil {
//		conf.Codec["type"] = "json"
//		codecHandler, _ := config.MapCodecHandler["json"]
//		conf.CodecPlugin, _ = codecHandler(ctx, conf.Codec)
//	}
//
//	return &conf, nil
//
//
//}

func (c *ConfigInput) Validate() error {
	if c.QueueUrl == "" {
		return fmt.Errorf("queue_url is require, input cloudtrail will stop")
	}


	if  c.VisibilityTimeout <= int64(0) || c.VisibilityTimeout > int64(43200) {
		return fmt.Errorf("visibility_timeout <%v> must be greater than 0 and "+
			"less than or equal to 12h", c.VisibilityTimeout)
	}

	if c.SQSWaitTime <= int64(0) || c.SQSWaitTime > int64(20) {
		return fmt.Errorf("wait_time <%v> must be greater than 0 and "+
			"less than or equal to 20s", c.SQSWaitTime)
	}

	if c.MaxNumberOfMessages <= 0 {
		return fmt.Errorf("max_number_of_messages <%v> must be greater than 0",
			c.MaxNumberOfMessages)
	}

	if c.ApiTimeout < c.SQSWaitTime {
		return fmt.Errorf("api_timeout <%v> must be greater than the sqs.wait_time <%v",
			c.ApiTimeout, c.SQSWaitTime)
	}

	return nil
}

func (c *ConfigInput) Run(ctx context.Context) error {
	if err := c.CreateSQSReceiver(); err != nil{
		return err
	}

	if err := c.Receive(ctx);err != nil{
		return err
	}

	return nil
}

func (c *ConfigInput) CreateSQSReceiver() error {
	awsConfig, err := c.InitializeAWSConfig()
	if err != nil {
		return err
	}
	regionName, err := getRegionFromQueueURL(c.QueueUrl, c.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to get AWS region from queue_url: %w", err)
	}
	awsConfig.Region = regionName


	c.sqsClient = sqs.New(c.EnrichAWSConfigWithEndpoint(c.Endpoint,"sqs",awsConfig.Region,awsConfig))
	c.s3Client = s3.New(c.EnrichAWSConfigWithEndpoint(c.Endpoint,"s3",awsConfig.Region,awsConfig))

	return nil

}

func (c *ConfigInput) Receive(ctx context.Context) error {
	var workerWg sync.WaitGroup
	var count int

	for ctx.Err() == nil {
		msgs, err := c.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() == nil {
				logrus.Warn("SQS ReceiveMessage returned an error. Will retry after a short delay.", "error", err)
				// Throttle retries.
				Wait(ctx, sqsRetryDelay)
			}
			continue
		}


	workerWg.Add(len(msgs))
		var muxtex sync.Mutex
	for _,msg := range msgs{

		go func(msg sqs.Message) {
			defer func() {
				workerWg.Done()
			}()
			muxtex.Lock()
			count++
			//logrus.Println("--------", count)
			if err := c.ProcessSQS(ctx, &msg,count); err != nil {
				logrus.Warn("Failed processing SQS message.", "error", err, "message_id", *msg.MessageId)
			}
			muxtex.Unlock()

		}(msg)
		}
	}
	workerWg.Wait()

	if errors.Is(ctx.Err(), context.Canceled) {
		// A canceled context is a normal shutdown.
		return nil
	}
	return ctx.Err()

}

func (c *ConfigInput)ReceiveMessage(ctx context.Context) ([]sqs.Message,error) {
	const sqsMaxNumberOfMessagesLimit = 10

	req := c.sqsClient.ReceiveMessageRequest(
		&sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.QueueUrl),
			MaxNumberOfMessages: aws.Int64(int64(min(c.MaxNumberOfMessages, sqsMaxNumberOfMessagesLimit))),
			VisibilityTimeout:   aws.Int64(c.VisibilityTimeout),
			WaitTimeSeconds:     aws.Int64(c.SQSWaitTime),
			AttributeNames:      []sqs.QueueAttributeName{sqsApproximateReceiveCountAttribute},
		})

	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.ApiTimeout) * time.Second )
	defer cancel()

	resp, err := req.Send(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("api_timeout exceeded: %w", err)
		}
		return nil, fmt.Errorf("sqs ReceiveMessage failed: %w", err)
	}

	return resp.Messages, nil

}

func getRegionFromQueueURL(queueURL string, endpoint string) (string, error) {
	// get region from queueURL
	// Example: https://sqs.us-east-1.amazonaws.com/627959692251/test-s3-logs
	url, err := url.Parse(queueURL)
	if err != nil {
		return "", fmt.Errorf(queueURL + " is not a valid URL")
	}
	if url.Scheme == "https" && url.Host != "" {
		queueHostSplit := strings.Split(url.Host, ".")
		if len(queueHostSplit) > 2 && (strings.Join(queueHostSplit[2:], ".") == endpoint || (endpoint == "" && queueHostSplit[2] == "amazonaws")) {
			return queueHostSplit[1], nil
		}
	}
	return "", fmt.Errorf("QueueURL is not in format: [https://sqs](https://sqs/).{REGION_ENDPOINT}.{ENDPOINT}/{ACCOUNT_NUMBER}/{QUEUE_NAME}")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type canceler interface {
	Done() <-chan struct{}
	Err() error
}

func Wait(ctx canceler, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}