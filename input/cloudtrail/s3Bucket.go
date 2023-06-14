package cloudtrail

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"io"
	event2 "jdbc/event"
	"net/http"
	"time"
)

func (c *ConfigInput) ProcessS3Object(ctx context.Context, obj s3EventV2,countE int,count int) error {
	// Request object (download).
	body, err := c.download(ctx, obj)
	if err != nil {
		return errors.Wrap(err, "failed to get s3 object")
	}
	defer body.Close()
	//p.s3Metadata = meta

	reader, err := c.addGzipDecoderIfNeeded(body)
	if err != nil {
		return errors.Wrap(err, "failed checking for gzip content")
	}

	// Overwrite with user configured Content-Type.
	//if p.readerConfig.ContentType != "" {
	//	contentType = p.readerConfig.ContentType
	//}

	// Process object content stream.
	err = c.readJSON(ctx,reader,obj,count)
	if err != nil {
		return err
	}

	return nil
}

func (p *ConfigInput) addGzipDecoderIfNeeded(body io.Reader) (io.Reader, error) {
	bufReader := bufio.NewReader(body)

	gzipped, err := isStreamGzipped(bufReader)
	if err != nil {
		return nil, err
	}
	if !gzipped {
		return bufReader, nil
	}

	return gzip.NewReader(bufReader)
}

// isStreamGzipped determines whether the given stream of bytes (encapsulated in a buffered reader)
// represents gzipped content or not. A buffered reader is used so the function can peek into the byte
// stream without consuming it. This makes it convenient for code executed after this function call
// to consume the stream if it wants.
func isStreamGzipped(r *bufio.Reader) (bool, error) {
	// Why 512? See https://godoc.org/net/http#DetectContentType
	buf, err := r.Peek(512)
	if err != nil && err != io.EOF {
		return false, err
	}

	switch http.DetectContentType(buf) {
	case "application/x-gzip", "application/zip":
		return true, nil
	default:
		return false, nil
	}
}

// download requests the S3 object from AWS and returns the object's
// Content-Type and reader to get the object's contents. The caller must
// close the returned reader.
func (c *ConfigInput) download(ctx context.Context,s3Obj s3EventV2) (  body io.ReadCloser, err error) {
	resp, err := c.GetObject(ctx, s3Obj.S3.Bucket.Name, s3Obj.S3.Object.Key)
	if err != nil {
		return  nil, err
	}

	if resp == nil {
		return  nil, errors.New("empty response from s3 get object")
	}

	return resp.Body, nil
}

func (c *ConfigInput) GetObject(ctx context.Context, bucket, key string) (*s3.GetObjectResponse, error) {
	req := c.s3Client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	resp, err := req.Send(ctx)
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject failed: %w", err)
	}
	return resp, nil
}

func (p *ConfigInput) readJSON(ctx context.Context,r io.Reader,obj s3EventV2,count int) error {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	var bashEvent []event2.Event
	for dec.More() && ctx.Err() == nil {
		//offset := dec.InputOffset()

		var item json.RawMessage
		if err := dec.Decode(&item); err != nil {
			return fmt.Errorf("failed to decode json: %w", err)
		}

		//if p.readerConfig.ExpandEventListFromField != "" {
		//	if err := p.splitEventList(p.readerConfig.ExpandEventListFromField, item, offset, p.s3ObjHash); err != nil {
		//		return err
		//	}
		//	continue
		//}

		data, _ := item.MarshalJSON()
		evt := createEvent(string(data), obj)

		bashEvent = append(bashEvent,evt)
		fmt.Println(bashEvent)
		fmt.Println("-----",count,"-----")

		//p.publish(p.acker, &evt)
	}
	return nil
}

func createEvent(message string, obj s3EventV2) event2.Event {
	event := map[string]interface{}{
		"Timestamp": time.Now().UTC(),
		"Fields": map[string]interface{}{
			"message": message,
			"log": map[string]interface{}{
				//"offset": offset,
				"file": map[string]interface{}{
					"path": constructObjectURL(obj),
				},
			},
			"aws": map[string]interface{}{
				"s3": map[string]interface{}{
					"bucket": map[string]interface{}{
						"name": obj.S3.Bucket.Name,
						"arn":  obj.S3.Bucket.ARN},
					"object": map[string]interface{}{
						"key": obj.S3.Object.Key,
					},
				},
			},
			"cloud": map[string]interface{}{
				"provider": "aws",
				"region":   obj.AWSRegion,
			},
		},
	}
	//event.SetID(objectID(objectHash, offset))
	//
	//if len(meta) > 0 {
	//	event.Fields.Put("aws.s3.metadata", meta)
	//}

	return event
}

func constructObjectURL(obj s3EventV2) string {
	return "https://" + obj.S3.Bucket.Name + ".s3." + obj.AWSRegion + ".amazonaws.com/" + obj.S3.Object.Key
}
