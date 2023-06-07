package cloudtrail

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/defaults"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestName(t *testing.T) {
	conf := config{
		APITimeout:          50,
		VisibilityTimeout:   50,
		SQSWaitTime:         20,
		SQSMaxReceiveCount:  10,
		FIPSEnabled:         false,
		MaxNumberOfMessages: 10,
		QueueURL:            "https://sqs.us-east-1.amazonaws.com/887134122148/aws-cloudtrail",
		AWSConfig:           ConfigAWS{
			AccessKeyID:          "AKIA45DKQOCSOWJR2LFX",
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
			MaxNumberOfMessages: aws.Int64(int64(10)),
			VisibilityTimeout:   aws.Int64(int64(200)),
			WaitTimeSeconds:     aws.Int64(int64(10)),
			AttributeNames:      []sqs.QueueAttributeName{sqsApproximateReceiveCountAttribute},
		})
	resp, err := req.Send(context.TODO())

	logrus.Println(err)
	logrus.Info(resp.Messages)

	for _,msg := range resp.Messages {
		logrus.Println(*msg.Body)
	}

	// sqsCl.ReceiveMessageInput{
	//	AttributeNames:          nil,
	//	MaxNumberOfMessages:     nil,
	//	MessageAttributeNames:   nil,
	//	QueueUrl:                nil,
	//	ReceiveRequestAttemptId: nil,
	//	VisibilityTimeout:       nil,
	//	WaitTimeSeconds:         nil,
	//}
}