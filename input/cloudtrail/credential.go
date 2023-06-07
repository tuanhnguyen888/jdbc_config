package cloudtrail

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/defaults"
	"github.com/aws/aws-sdk-go-v2/aws/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/sirupsen/logrus"
	"net/http"
)

type ConfigAWS struct {
	AccessKeyID          string `config:"access_key_id"`
	SecretAccessKey      string `config:"secret_access_key"`
	SessionToken         string `config:"session_token"`
	ProfileName          string `config:"credential_profile_name"`
	SharedCredentialFile string `config:"shared_credential_file"`
	Endpoint             string `config:"endpoint"`
	RoleArn              string `config:"role_arn"`
	AWSPartition         string `config:"aws_partition"` // Deprecated.
	ProxyUrl             string `config:"proxy_url"`
}


func InitializeAWSConfig(config ConfigAWS) (aws.Config, error) {
	AWSConfig, _ := GetAWSCredentials(config)
	if config.ProxyUrl != "" {
		proxyUrl, err := NewProxyURIFromString(config.ProxyUrl)
		if err != nil {
			return AWSConfig, err
		}

		httpClient := &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyURL(proxyUrl.URI()),
			},
		}
		AWSConfig.HTTPClient = httpClient
	}
	return AWSConfig, nil
}

func GetAWSCredentials(config ConfigAWS) (aws.Config, error) {
	// Check if accessKeyID or secretAccessKey or sessionToken is given from configuration
	return getAccessKeys(config), nil
}

func getAccessKeys(config ConfigAWS) aws.Config {
	//logger := logp.NewLogger("getAccessKeys")
	awsConfig := defaults.Config()
	awsCredentials := aws.Credentials{
		AccessKeyID:     config.AccessKeyID,
		SecretAccessKey: config.SecretAccessKey,
	}

	if config.SessionToken != "" {
		awsCredentials.SessionToken = config.SessionToken
	}

	awsConfig.Credentials = aws.StaticCredentialsProvider{
		Value: awsCredentials,
	}

	// Set default region if empty to make initial aws api call
	if awsConfig.Region == "" {
		awsConfig.Region = "us-east-1"
	}

	// Assume IAM role if iam_role config parameter is given
	if config.RoleArn != "" {
		logrus.Debug("Using role arn and access keys for AWS credential")
		return getRoleArn(config, awsConfig)
	}

	return awsConfig
}

func getRoleArn(config ConfigAWS, awsConfig aws.Config) aws.Config {
	stsSvc := sts.New(awsConfig)
	stsCredProvider := stscreds.NewAssumeRoleProvider(stsSvc, config.RoleArn)
	awsConfig.Credentials = stsCredProvider
	return awsConfig
}

func EnrichAWSConfigWithEndpoint(endpoint string, serviceName string, regionName string, awsConfig aws.Config) aws.Config {
	if endpoint != "" {
		if regionName == "" {
			awsConfig.EndpointResolver = aws.ResolveWithEndpointURL("https://" + serviceName + "." + endpoint)
		} else {
			awsConfig.EndpointResolver = aws.ResolveWithEndpointURL("https://" + serviceName + "." + regionName + "." + endpoint)
		}
	}
	return awsConfig
}