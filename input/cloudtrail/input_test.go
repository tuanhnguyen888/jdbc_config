package cloudtrail

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	event2 "jdbc/event"
	"net/url"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	conf := &ConfigInput{
		ApiTimeout:          21 ,
		VisibilityTimeout:   300 ,
		SQSWaitTime:         20 ,
		MaxNumberOfMessages: 5,
		Endpoint: "amazonaws.com",
		DefaultRegion: "us-east-1",
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/887134122148/aws-cloudtrail",
		AccessKeyId:          "AKIA45DKQOCSHDLUAQ7H",
		SecretAccessKey:      "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",
		//SessionToken:         "",
		//ProxyUrl: "http://161.117.177.202:3128",
		//RoleArn:              "arn:aws:iam::887134122148:role/aws-service-role/organizations.amazonaws.com/AWSServiceRoleForOrganizations",
	}

	err := conf.Validate()
	assert.NoError(t, err)
	ctx := context.Background()
	go func() {
		err = conf.Run(ctx)
	}()
	time.Sleep(20 * time.Second)
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

func TestCreate(t *testing.T) {
	message := "message testing"
	obj := s3EventV2{
		AWSRegion:   "us",
		EventName:   "pushObj",
		EventSource: "",
		S3: struct {
			Bucket struct {
				Name string `json:"name"`
				ARN  string `json:"arn"`
			} `json:"bucket"`
			Object struct {
				Key string `json:"key"`
			} `json:"object"`
		}{},
	}

	event := CreateEvent(message,obj)
	assert.IsType(t, event2.Event{},event)
	logrus.Println(event)
}

func TestUrl(t *testing.T) {
	urlStr, e := url.Parse("akdjadk.com:ada")

	if e != nil {
		fmt.Println("Yes! I,m check this error!, url ; ",e, urlStr)
	} else {
		fmt.Println("NO! I broken and kill your production, grrrrr:  ", urlStr)
	}
}