package cloudtrail

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/defaults"
	"github.com/aws/aws-sdk-go-v2/aws/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/sirupsen/logrus"
	"net/http"
	"net/url"
)

func (c *ConfigInput) InitializeAWSConfig() (aws.Config, error) {
	AWSConfig, _ := c.GetAWSCredentials()
	//Add ProxyUrl
	if c.ProxyUrl != "" {
		proxyURL, err := url.Parse(c.ProxyUrl)
		if err != nil {
			fmt.Printf("Error parsing proxy URL: %s\\n", err)
			return AWSConfig,err
		}
		httpClient := &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyURL(proxyURL),
			},
		}
		logrus.Println(proxyURL)
		AWSConfig.HTTPClient = httpClient
	}
	return AWSConfig, nil
}

func (c *ConfigInput)GetAWSCredentials() (aws.Config, error) {
	// Check if accessKeyID or secretAccessKey or sessionToken is given from configuration
	return c.getAccessKeys(),nil
}

func (c *ConfigInput)getAccessKeys() aws.Config {
	awsConfig := defaults.Config()
	awsCredentials := aws.Credentials{
		AccessKeyID:     c.AccessKeyId,
		SecretAccessKey: c.SecretAccessKey,
	}


if c.SessionToken != "" {
	awsCredentials.SessionToken = c.SessionToken
}

awsConfig.Credentials = aws.StaticCredentialsProvider{
	Value: awsCredentials,
}

// Set default region if empty to make initial aws api call
awsConfig.Region = c.DefaultRegion

// Assume IAM role if iam_role config parameter is given
if c.RoleArn != "" {
	logrus.Debug("Using role arn and access keys for AWS credential")
	return c.getRoleArn(awsConfig)
}

return awsConfig



}

func (c *ConfigInput)getRoleArn( awsConfig aws.Config) aws.Config {
	stsSvc := sts.New(awsConfig)
	stsCredProvider := stscreds.NewAssumeRoleProvider(stsSvc, c.RoleArn)
	awsConfig.Credentials = stsCredProvider
	return awsConfig
}

func (c *ConfigInput)EnrichAWSConfigWithEndpoint(endpoint string, serviceName string, regionName string, awsConfig aws.Config) aws.Config {
	if endpoint != "" {
		if regionName == "" {
			awsConfig.EndpointResolver = aws.ResolveWithEndpointURL("https://" + serviceName + "." + endpoint)
		} else {
			awsConfig.EndpointResolver = aws.ResolveWithEndpointURL("https://" + serviceName + "." + regionName + "." + endpoint)
		}
	}
	return awsConfig
}