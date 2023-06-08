package cloudtrail

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"net/url"
	"strings"
	"sync"
	"time"
)
const (
	sqsApproximateReceiveCountAttribute = "ApproximateReceiveCount"
)

type s3EventsV2 struct {
	TopicArn string      `json:"TopicArn"`
	Message  string      `json:"Message"`
	Records  []s3EventV2 `json:"Records"`
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

type sqsS3EventProcessor struct {
	s3ObjectHandler      s3ObjectHandlerFactory
	sqsVisibilityTimeout time.Duration
	maxReceiveCount      int
	sqs                  sqsAPI
	//log                  *logp.Logger
	warnOnce             sync.Once
	//metrics              *inputMetrics
	//script               *script
}

func newSQSS3EventProcessor(  sqs sqsAPI, sqsVisibilityTimeout time.Duration, maxReceiveCount int) *sqsS3EventProcessor {
	//if metrics == nil {
	//	metrics = newInputMetrics(monitoring.NewRegistry(), "")
	//}
	return &sqsS3EventProcessor{
		//s3ObjectHandler:      s3,
		sqsVisibilityTimeout: sqsVisibilityTimeout,
		maxReceiveCount:      maxReceiveCount,
		sqs:                  sqs,
		//log:                  log,
		//metrics:              metrics,
		//script:               script,
	}
}

func (p *sqsS3EventProcessor) ProcessSQS(ctx context.Context, msg *sqs.Message) error {
	//log := p.log.With("message_id", *msg.MessageId)

	keepaliveCtx, keepaliveCancel := context.WithCancel(ctx)
	defer keepaliveCancel()

	// Start SQS keepalive worker.
	var keepaliveWg sync.WaitGroup
	keepaliveWg.Add(1)
	go p.keepalive(keepaliveCtx, &keepaliveWg, msg)

	processingErr := p.processS3Events(ctx, *msg.Body)

	// Stop keepalive routine before changing visibility.
	keepaliveCancel()
	keepaliveWg.Wait()

	// No error. Delete SQS.
	if processingErr == nil {
		msgDelErr := p.sqs.DeleteMessage(context.Background(), msg)
		//if msgDelErr == nil {
		//	p.metrics.sqsMessagesDeletedTotal.Inc()
		//}
		return errors.Wrap(msgDelErr, "failed deleting message from SQS queue (it may be reprocessed)")
	}

	//if p.maxReceiveCount > 0 && !errors.Is(processingErr, &nonRetryableError{}) {
	//	// Prevent poison pill messages from consuming all workers. Check how
	//	// many times this message has been received before making a disposition.
	//	if v, found := msg.Attributes[sqsApproximateReceiveCountAttribute]; found {
	//		if receiveCount, err := strconv.Atoi(v); err == nil && receiveCount >= p.maxReceiveCount {
	//			processingErr = nonRetryableErrorWrap(fmt.Errorf(
	//				"sqs ApproximateReceiveCount <%v> exceeds threshold %v: %w",
	//				receiveCount, p.maxReceiveCount, err))
	//		}
	//	}
	//}
	//
	//// An error that reprocessing cannot correct. Delete SQS.
	//if errors.Is(processingErr, &nonRetryableError{}) {
	//	msgDelErr := p.sqs.DeleteMessage(context.Background(), msg)
	//	if msgDelErr == nil {
	//		p.metrics.sqsMessagesDeletedTotal.Inc()
	//	}
	//	return multierr.Combine(
	//		errors.Wrap(processingErr, "failed processing SQS message (message will be deleted)"),
	//		errors.Wrap(msgDelErr, "failed deleting message from SQS queue (it may be reprocessed)"),
	//	)
	//}

	// An error that may be resolved by letting the visibility timeout
	// expire thereby putting the message back on SQS. If a dead letter
	// queue is enabled then the message will eventually placed on the DLQ
	// after maximum receives is reached.

	//p.metrics.sqsMessagesReturnedTotal.Inc()
	return errors.Wrap(processingErr, "failed processing SQS message (it will return to queue after visibility timeout)")
}

func (p *sqsS3EventProcessor) keepalive(ctx context.Context, wg *sync.WaitGroup, msg *sqs.Message) {
	defer wg.Done()

	t := time.NewTicker(p.sqsVisibilityTimeout / 2)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			logrus.Debug("Extending SQS message visibility timeout.",
				"visibility_timeout", p.sqsVisibilityTimeout,
				"expires_at", time.Now().UTC().Add(p.sqsVisibilityTimeout))
			//p.metrics.sqsVisibilityTimeoutExtensionsTotal.Inc()

			// Renew visibility.
			if err := p.sqs.ChangeMessageVisibility(ctx, msg, p.sqsVisibilityTimeout); err != nil {
				logrus.Warn("Failed to extend message visibility timeout.", "error", err)
			}
		}
	}
}

func (p *sqsS3EventProcessor) getS3Notifications(body string) ([]s3EventV2, error) {
	// Check if a parsing script is defined. If so, it takes precedence over
	// format autodetection.
	//if p.script != nil {
	//	return p.script.run(body)
	//}

	// NOTE: If AWS introduces a V3 schema this will need updated to handle that schema.
	var events s3EventsV2
	dec := json.NewDecoder(strings.NewReader(body))
	if err := dec.Decode(&events); err != nil {
		logrus.Debug("Invalid SQS message body.", "sqs_message_body", body)
		return nil, fmt.Errorf("failed to decode SQS message body as an S3 notification: %w", err)
	}

	// Check if the notification is from S3 -> SNS -> SQS
	if events.TopicArn != "" {
		dec := json.NewDecoder(strings.NewReader(events.Message))
		if err := dec.Decode(&events); err != nil {
			logrus.Debug("Invalid SQS message body.", "sqs_message_body", body)
			return nil, fmt.Errorf("failed to decode SQS message body as an S3 notification: %w", err)
		}
	}

	if events.Records == nil {
		logrus.Debug("Invalid SQS message body: missing Records field", "sqs_message_body", body)
		return nil, errors.New("the message is an invalid S3 notification: missing Records field")
	}

	return p.getS3Info(events)
}

func (p *sqsS3EventProcessor) getS3Info(events s3EventsV2) ([]s3EventV2, error) {
	var out []s3EventV2
	for _, record := range events.Records {
		if !p.isObjectCreatedEvents(record) {
			p.warnOnce.Do(func() {
				logrus.Warnf("Received S3 notification for %q event type, but "+
					"only 'ObjectCreated:*' types are handled. It is recommended "+
					"that you update the S3 Event Notification configuration to "+
					"only include ObjectCreated event types to save resources.",
					record.EventName)
			})
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

func (_ *sqsS3EventProcessor) isObjectCreatedEvents(event s3EventV2) bool {
	return event.EventSource == "aws:s3" && strings.HasPrefix(event.EventName, "ObjectCreated:")
}

func (p *sqsS3EventProcessor) processS3Events(ctx context.Context, body string) error {
	s3Events, err := p.getS3Notifications(body)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// Messages that are in-flight at shutdown should be returned to SQS.
			return err
		}
		return err
	}
	logrus.Debugf("SQS message contained %d S3 event notifications.", len(s3Events))
	defer logrus.Debugf("End processing SQS S3 event notifications.")

	// Wait for all events to be ACKed before proceeding.
	//acker := newEventACKTracker(ctx)
	//defer acker.Wait()

	//var errs []error
	for _, event := range s3Events {
		//s3Processor := p.s3ObjectHandler.Create(ctx, log, acker, event)
		//if s3Processor == nil {
		//	continue
		//}
		//
		//// Process S3 object (download, parse, create events).
		//if err := s3Processor.ProcessS3Object(); err != nil {
		//	errs = append(errs, errors.Wrapf(err,
		//		"failed processing S3 event for object key %q in bucket %q",
		//		event.S3.Object.Key, event.S3.Bucket.Name))
		//}
		logrus.Info(event)
	}

	return nil
}
