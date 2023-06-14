package cloudtrail

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)
type s3EventsV2 struct {
	Records []s3EventV2 `json:"Records"`
}

// s3EventV2 is a S3 change notification event.
type s3EventV2 struct {
	AWSRegion   string `json:"awsRegion"`
	EventName   string `json:"eventName"`
	EventSource string `json:"eventSource"`
	S3          struct {
		Bucket struct {
			Name string `json:"name"`
			ARN  string `json:"arn"`
		} `json:"bucket"`
		Object struct {
			Key string `json:"key"`
		} `json:"object"`
	} `json:"s3"`
}

func (c *ConfigInput) ProcessSQS(ctx context.Context, msg *sqs.Message,count int)error  {
	keepaliveCtx, keepaliveCancel := context.WithCancel(ctx)
	defer keepaliveCancel()


	// Start SQS keepalive worker.
	var keepaliveWg sync.WaitGroup
	keepaliveWg.Add(1)
	go c.keepalive(keepaliveCtx, &keepaliveWg, msg)

	processingErr := c.processS3Events(ctx, *msg.Body,count)

	// Stop keepalive routine before changing visibility.
	keepaliveCancel()
	keepaliveWg.Wait()

	// No error. Delete SQS.
	if processingErr == nil {
		msgDelErr := c.DeleteMessage(context.Background(), msg)
		//if msgDelErr == nil {}
		return errors.Wrap(msgDelErr, "failed deleting message from SQS queue (it may be reprocessed)")
}

	if c.SQSMaxReceiveCount > 0 {
		// Prevent poison pill messages from consuming all workers. Check how
		// many times this message has been received before making a disposition.
		if v, found := msg.Attributes[sqsApproximateReceiveCountAttribute]; found {
			if receiveCount, err := strconv.Atoi(v); err == nil && receiveCount >= c.SQSMaxReceiveCount {
				processingErr = fmt.Errorf("sqs ApproximateReceiveCount <%v> exceeds threshold %v: %w", receiveCount, c.SQSMaxReceiveCount, err)
				msgDelErr := c.DeleteMessage(context.Background(), msg)
				if msgDelErr != nil {
					return  errors.Wrap(msgDelErr, "failed deleting message from SQS queue (it may be reprocessed)")
				}
				return errors.Wrap(processingErr, "failed processing SQS message (message will be deleted)")
			}
		}
	}
	return errors.Wrap(processingErr, "failed processing SQS message (it will return to queue after visibility timeout)")
}

func (c *ConfigInput) keepalive(ctx context.Context,  wg *sync.WaitGroup, msg *sqs.Message) {
	defer wg.Done()
	t := time.NewTicker((time.Duration(c.VisibilityTimeout)*time.Second) / 2)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			logrus.Warn("extending SQS message visibility timeout.", "visibility_timeout", c.VisibilityTimeout, "expires_at", time.Now().UTC().Add(time.Duration(c.VisibilityTimeout)*time.Second))
			// Renew visibility.
			if err := c.ChangeMessageVisibility(ctx, msg, time.Duration(c.VisibilityTimeout)*time.Second); err != nil {
				logrus.Warn("Failed to extend message visibility timeout.", "error", err)
			}
		}
	}

}

func (c *ConfigInput) ChangeMessageVisibility(ctx context.Context, msg *sqs.Message, timeout time.Duration) error {
	req := c.sqsClient.ChangeMessageVisibilityRequest(
		&sqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(c.QueueUrl),
			ReceiptHandle:     msg.ReceiptHandle,
			VisibilityTimeout: aws.Int64(int64(timeout.Seconds())),
		})


	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.ApiTimeout)*time.Second)
	defer cancel()

	if _, err := req.Send(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("api_timeout exceeded: %w", err)
		}
		return fmt.Errorf("sqs ChangeMessageVisibility failed: %w", err)
	}

	return nil
}

func (c *ConfigInput) DeleteMessage(ctx context.Context, msg *sqs.Message) error {
	req := c.sqsClient.DeleteMessageRequest(
		&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(c.QueueUrl),
			ReceiptHandle: msg.ReceiptHandle,
		})


	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.ApiTimeout)*time.Second)
	defer cancel()

	if _, err := req.Send(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("api_timeout exceeded: %w", err)
		}
		return fmt.Errorf("sqs DeleteMessage failed: %w", err)
	}

	return nil

}

func (c *ConfigInput) processS3Events(ctx context.Context, body string,count int) error {
	s3Events, err := c.getS3Notifications(body)
	if err != nil {
		return err
	}


	if len(s3Events) == 0 {
		return nil
	}
	fmt.Println("\n ---------  ", count)
	logrus.Debugf("SQS message contained %d S3 event notifications.", len(s3Events))
	defer logrus.Debug("End processing SQS S3 event notifications.")


	// Wait for all events to be ACKed before proceeding.
	var countEvent int
	for _, event := range s3Events {
		countEvent ++
		// Process S3 object (download, parse, create events)
		logrus.Printf("%v ---- %#v",count,event)
		c.ProcessS3Object(ctx,event,countEvent,count)

	}
	return nil
}

func (p *ConfigInput) getS3Notifications(body string) ([]s3EventV2, error) {
	// NOTE: If AWS introduces a V3 schema this will need updated to handle that schema.
	var events s3EventsV2
	dec := json.NewDecoder(strings.NewReader(body))
	if err := dec.Decode(&events); err != nil {
		logrus.Debug("Invalid SQS message body.", "sqs_message_body", body)
		return nil, fmt.Errorf("failed to decode SQS message body as an S3 notification: %w", err)
	}

	var out []s3EventV2
	for _, record := range events.Records {
		if !p.isObjectCreatedEvents(record) {
			logrus.Warnf("Received S3 notification for %q event type, but "+
				"only 'ObjectCreated:*' types are handled. It is recommended "+
				"that you update the S3 Event Notification configuration to "+
				"only include ObjectCreated event types to save resources.",
				record.EventName)
			continue
			}

		// Unescape s3 key name. For example, convert "%3D" back to "=".
		key, err := url.QueryUnescape(record.S3.Object.Key)
		if err != nil {
			return nil, fmt.Errorf("url unescape failed for '%v': %w", record.S3.Object.Key, err)
		}
		record.S3.Object.Key = key

		out = append(out, record)
	}

	return out, nil

}

func (_ *ConfigInput) isObjectCreatedEvents(event s3EventV2) bool {
	return event.EventSource == "aws:s3" && strings.HasPrefix(event.EventName, "ObjectCreated:")
}