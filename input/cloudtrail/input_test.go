package cloudtrail

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/defaults"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net/url"
	"strings"
	"testing"
)

func TestName(t *testing.T) {
	conf := config{
		APITimeout:          50,
		VisibilityTimeout:   50,
		SQSWaitTime:         20,
		SQSMaxReceiveCount:  10,
		FIPSEnabled:         false,
		MaxNumberOfMessages: 2,
		QueueURL:            "https://sqs.us-east-1.amazonaws.com/887134122148/aws-cloudtrail",
		AWSConfig:           ConfigAWS{
			AccessKeyID:          "AKIA45DKQOCSAB3M5HIH",
			SecretAccessKey:      "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",
			SessionToken:         "",
			Endpoint:             "",
			//RoleArn:              "arn:aws:iam::887134122148:role/aws-service-role/organizations.amazonaws.com/AWSServiceRoleForOrganizations",
			AWSPartition:         "",
			ProxyUrl:             "",
		},
	}

	s3 ,err := newInput(conf)
	assert.NoError(t, err)

	err = s3.Run()
	logrus.Error(err)
	assert.Error(t, err)
}
//

func TestReal(t *testing.T) {
	conf := ConfigAWS{
		AccessKeyID:          "AKIA45DKQOCSAB3M5HIH",
		SecretAccessKey:      "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",
		SessionToken:         "",
		Endpoint:             "",
		RoleArn:              "arn:aws:iam::887134122148:role/aws-service-role/organizations.amazonaws.com/AWSServiceRoleForOrganizations",
	}

	awsConfig := defaults.Config()
	awsCredentials := aws.Credentials{
		AccessKeyID:     conf.AccessKeyID,
		SecretAccessKey: conf.SecretAccessKey,
	}
	awsConfig.Region = "us-east-1"
	awsConfig.Credentials = aws.StaticCredentialsProvider{
		Value: awsCredentials,
	}

	//role
	//stsSvc := sts.New(awsConfig)
	//stsCredProvider := stscreds.NewAssumeRoleProvider(stsSvc, conf.RoleArn)
	//awsConfig.Credentials = stsCredProvider

	fmt.Println(awsConfig)

	sqsCl := sqs.New(awsConfig)
	req := sqsCl.ReceiveMessageRequest(
		&sqs.ReceiveMessageInput{
			QueueUrl:            aws.String("https://sqs.us-east-1.amazonaws.com/887134122148/aws-cloudtrail"),
			MaxNumberOfMessages: aws.Int64(int64(2)),
			VisibilityTimeout:   aws.Int64(int64(200)),
			WaitTimeSeconds:     aws.Int64(int64(20)),
			AttributeNames:      []sqs.QueueAttributeName{sqsApproximateReceiveCountAttribute},
		})
	resp, err := req.Send(context.TODO())

	logrus.Println(err)
	logrus.Info(resp.Messages)

	for _,msg := range resp.Messages {
		jsonStr := *msg.Body

		var events s3EventsV2
		dec := json.NewDecoder(strings.NewReader(jsonStr))
		 _ = dec.Decode(&events)
		fmt.Printf("%#v\n", events)
		var out []s3EventV2
		for _, record := range events.Records {
			// Unescape s3 key name. For example, convert "%3D" back to "=".
			key, _ := url.QueryUnescape(record.S3.Object.Key)
			record.S3.Object.Key = key
			out = append(out, record)
			fmt.Println("Event: ", record)
		}
		logrus.Warn("Events: ", out)

		reqDel := sqsCl.DeleteMessageRequest(
			&sqs.DeleteMessageInput{
				QueueUrl:      aws.String("https://sqs.us-east-1.amazonaws.com/887134122148/aws-cloudtrail"),
				ReceiptHandle: msg.ReceiptHandle,
			})

		_, err = reqDel.Send(context.TODO())
		assert.NoError(t, err)
	}


}