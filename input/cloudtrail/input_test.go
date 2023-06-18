package cloudtrail

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	conf := &ConfigInput{
		ApiTimeout:          120 ,
		VisibilityTimeout:   300 ,
		SQSWaitTime:         20 ,
		MaxNumberOfMessages: 5,
		Endpoint: "amazonaws.com",
		DefaultRegion: "us-east-1",
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/887134122148/aws-cloudtrail",
		AccessKeyId:          "AKIA45DKQOCSAB3M5HIH",
		SecretAccessKey:      "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",
		SessionToken:         "",
		//RoleArn:              "arn:aws:iam::887134122148:role/aws-service-role/organizations.amazonaws.com/AWSServiceRoleForOrganizations",
	}

	err := conf.Validate()
	assert.NoError(t, err)
	ctx := context.Background()
	go func() {
		err = conf.Run(ctx)
	}()
	time.Sleep(50 * time.Second)
	ctx.Done()
	assert.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
}
//

func FakeGetObjectResponse() (*s3.GetObjectResponse, error) {
	// Tạo dữ liệu body giả.
	bodyData := []byte("fake body content")

	// Tạo một buffer từ dữ liệu body giả.
	buffer := bytes.NewBuffer(bodyData)

	// Tạo một đối tượng *s3.GetObjectResponse giả lập.
	resBody := &s3.GetObjectOutput{Body:ioutil.NopCloser(buffer) }
	response := &s3.GetObjectResponse{
		GetObjectOutput : resBody,
	}

	return response, nil
}

func TestBody (t *testing.T) {
	conf := &ConfigInput{
		ApiTimeout:          120 ,
		VisibilityTimeout:   300 ,
		SQSWaitTime:         20 ,
		MaxNumberOfMessages: 5,
		Endpoint: "amazonaws.com",
		DefaultRegion: "us-east-1",
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/887134122148/aws-cloudtrail",
		AccessKeyId:          "AKIA45DKQOCSAB3M5HIH",
		SecretAccessKey:      "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",
		SessionToken:         "",
		//RoleArn:              "arn:aws:iam::887134122148:role/aws-service-role/organizations.amazonaws.com/AWSServiceRoleForOrganizations",
	}
	body, _ := FakeGetObjectResponse()
	zip , err := conf.AddGzipDecoderIfNeeded(body.Body)
	assert.NoError(t, err)
	logrus.Println(zip,err)

	 err = conf.ReadJSON(context.TODO(),zip,s3EventV2{},1)
	 assert.NoError(t, err)

}
