package cloudtrail

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/sirupsen/logrus"
	"net/url"
	"strings"
)


type s3Input struct {
	config    config
	awsConfig aws.Config
	//store     beater.StateStore
}

func newInput(config config) (*s3Input, error) {
	awsConfig, err := InitializeAWSConfig(config.AWSConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize AWS credentials: %w", err)
	}

	return &s3Input{
		config:    config,
		awsConfig: awsConfig,
		//store:     store,
	}, nil
}

func (s *s3Input) Run()  error {
	//var err error
	ctx, cancelInputCtx := context.WithCancel(context.Background())
	go func() {
		defer cancelInputCtx()
		select {
		//case <-inputContext.Cancelation.Done():
		case <-ctx.Done():
		}
	}()

	defer cancelInputCtx()
	if s.config.QueueURL != "" {

		regionName, err := getRegionFromQueueURL(s.config.QueueURL, s.config.AWSConfig.Endpoint)
		if err != nil {
			return fmt.Errorf("failed to get AWS region from queue_url: %w", err)
		}
		s.awsConfig.Region = regionName

		// Create SQS receiver and S3 notification processor.
		receiver, err := s.createSQSReceiver()
		if err != nil {
			return fmt.Errorf("failed to initialize sqs receiver: %w", err)
		}
		//defer receiver.metrics.Close()

		if err := receiver.Receive(ctx); err != nil {
			return err
		}
	}

	return fmt.Errorf("QueueURL is required")
}


func (in *s3Input) createSQSReceiver() (*sqsReader, error) {
	s3ServiceName := "s3"
	if in.config.FIPSEnabled {
		s3ServiceName = "s3-fips"
	}

	sqsAPI := &awsSQSAPI{
		client:            sqs.New(EnrichAWSConfigWithEndpoint(in.config.AWSConfig.Endpoint, "sqs", in.awsConfig.Region, in.awsConfig)),
		queueURL:          in.config.QueueURL,
		apiTimeout:        in.config.APITimeout,
		visibilityTimeout: in.config.VisibilityTimeout,
		longPollWaitTime:  in.config.SQSWaitTime,
	}

	//s3API := &awsS3API{
	//	client: s3.New(EnrichAWSConfigWithEndpoint(in.config.AWSConfig.Endpoint, s3ServiceName, in.awsConfig.Region, in.awsConfig)),
	//}

	//log := ctx.Logger.With("queue_url", in.config.QueueURL)
	logrus.Infof("AWS api_timeout is set to %v.", in.config.APITimeout)
	logrus.Infof("AWS region is set to %v.", in.awsConfig.Region)
	logrus.Infof("AWS SQS visibility_timeout is set to %v.", in.config.VisibilityTimeout)
	logrus.Infof("AWS SQS max_number_of_messages is set to %v.", in.config.MaxNumberOfMessages)
	logrus.Debugf("AWS S3 service name is %v.", s3ServiceName)

	//metricRegistry := monitoring.GetNamespace("dataset").GetRegistry()
	//metrics := newInputMetrics(metricRegistry, ctx.ID)

	//fileSelectors := in.config.FileSelectors
	//if len(in.config.FileSelectors) == 0 {
	//	fileSelectors = []fileSelectorConfig{{ReaderConfig: in.config.ReaderConfig}}
	//}
	//script, err := newScriptFromConfig(log.Named("sqs_script"), in.config.SQSScript)
	//if err != nil {
	//	return nil, err
	//}
	//s3EventHandlerFactory := newS3ObjectProcessorFactory(log.Named("s3"), metrics, s3API, client, fileSelectors)

	sqsMessageHandler := newSQSS3EventProcessor( sqsAPI, in.config.VisibilityTimeout, in.config.SQSMaxReceiveCount)

	sqsReader := newSQSReader( sqsAPI, in.config.MaxNumberOfMessages, sqsMessageHandler)

	return sqsReader, nil
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
	return "", fmt.Errorf("QueueURL is not in format: https://sqs.{REGION_ENDPOINT}.{ENDPOINT}/{ACCOUNT_NUMBER}/{QUEUE_NAME}")
}