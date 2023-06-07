package cloudtrail

import (
	"context"
	"errors"
	"fmt"
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"time"
)


// ------
// SQS interfaces
// ------

type sqsAPI interface {
	sqsReceiver
	sqsDeleter
	sqsVisibilityChanger
}

type sqsReceiver interface {
	ReceiveMessage(ctx context.Context, maxMessages int) ([]sqs.Message, error)
}

type sqsDeleter interface {
	DeleteMessage(ctx context.Context, msg *sqs.Message) error
}

type sqsVisibilityChanger interface {
	ChangeMessageVisibility(ctx context.Context, msg *sqs.Message, timeout time.Duration) error
}

type sqsProcessor interface {
	// ProcessSQS processes and SQS message. It takes fully ownership of the
	// given message and is responsible for updating the message's visibility
	// timeout while it is being processed and for deleting it when processing
	// completes successfully.
	ProcessSQS(ctx context.Context, msg *sqs.Message) error
}

// ------
// S3 interfaces
// ------

type s3API interface {
	s3Getter
	s3Lister
}

type s3Getter interface {
	GetObject(ctx context.Context, bucket, key string) (*s3.GetObjectResponse, error)
}

type s3Lister interface {
	ListObjectsPaginator(bucket, prefix string) s3Pager
}

type s3Pager interface {
	Next(ctx context.Context) bool
	CurrentPage() *s3.ListObjectsOutput
	Err() error
}

type s3ObjectHandlerFactory interface {
	// Create returns a new s3ObjectHandler that can be used to process the
	// specified S3 object. If the handler is not configured to process the
	// given S3 object (based on key name) then it will return nil.
	//Create(ctx context.Context, log *logp.Logger, acker *eventACKTracker, obj s3EventV2) s3ObjectHandler
}

type s3ObjectHandler interface {
	// ProcessS3Object downloads the S3 object, parses it, creates events, and
	// publishes them. It returns when processing finishes or when it encounters
	// an unrecoverable error. It does not wait for the events to be ACKed by
	// the publisher before returning (use eventACKTracker's Wait() method to
	// determine this).
	ProcessS3Object() error

	// Wait waits for every event published by ProcessS3Object() to be ACKed
	// by the publisher before returning. Internally it uses the
	// s3ObjectHandler eventACKTracker's Wait() method
	Wait()
}

// ------
// AWS SQS implementation
// ------

type awsSQSAPI struct {
	client            *sqs.Client
	queueURL          string
	apiTimeout        time.Duration
	visibilityTimeout time.Duration
	longPollWaitTime  time.Duration
}

func (a *awsSQSAPI) ReceiveMessage(ctx context.Context, maxMessages int) ([]sqs.Message, error) {
	const sqsMaxNumberOfMessagesLimit = 10

	req := a.client.ReceiveMessageRequest(
		&sqs.ReceiveMessageInput{
			QueueUrl:            awssdk.String(a.queueURL),
			MaxNumberOfMessages: awssdk.Int64(int64(min(maxMessages, sqsMaxNumberOfMessagesLimit))),
			VisibilityTimeout:   awssdk.Int64(int64(a.visibilityTimeout.Seconds())),
			WaitTimeSeconds:     awssdk.Int64(int64(a.longPollWaitTime.Seconds())),
			AttributeNames:      []sqs.QueueAttributeName{sqsApproximateReceiveCountAttribute},
		})

	ctx, cancel := context.WithTimeout(ctx, a.apiTimeout)
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (a *awsSQSAPI) DeleteMessage(ctx context.Context, msg *sqs.Message) error {
	req := a.client.DeleteMessageRequest(
		&sqs.DeleteMessageInput{
			QueueUrl:      awssdk.String(a.queueURL),
			ReceiptHandle: msg.ReceiptHandle,
		})

	ctx, cancel := context.WithTimeout(ctx, a.apiTimeout)
	defer cancel()

	if _, err := req.Send(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("api_timeout exceeded: %w", err)
		}
		return fmt.Errorf("sqs DeleteMessage failed: %w", err)
	}

	return nil
}

func (a *awsSQSAPI) ChangeMessageVisibility(ctx context.Context, msg *sqs.Message, timeout time.Duration) error {
	req := a.client.ChangeMessageVisibilityRequest(
		&sqs.ChangeMessageVisibilityInput{
			QueueUrl:          awssdk.String(a.queueURL),
			ReceiptHandle:     msg.ReceiptHandle,
			VisibilityTimeout: awssdk.Int64(int64(timeout.Seconds())),
		})

	ctx, cancel := context.WithTimeout(ctx, a.apiTimeout)
	defer cancel()

	if _, err := req.Send(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("api_timeout exceeded: %w", err)
		}
		return fmt.Errorf("sqs ChangeMessageVisibility failed: %w", err)
	}

	return nil
}

// ------
// AWS S3 implementation
// ------

type awsS3API struct {
	client *s3.Client
}

func (a *awsS3API) GetObject(ctx context.Context, bucket, key string) (*s3.GetObjectResponse, error) {
	req := a.client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: awssdk.String(bucket),
		Key:    awssdk.String(key),
	})

	resp, err := req.Send(ctx)
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject failed: %w", err)
	}

	return resp, nil
}

func (a *awsS3API) ListObjectsPaginator(bucket, prefix string) s3Pager {
	req := a.client.ListObjectsRequest(&s3.ListObjectsInput{
		Bucket: awssdk.String(bucket),
		Prefix: awssdk.String(prefix),
	})

	pager := s3.NewListObjectsPaginator(req)
	return &pager
}
