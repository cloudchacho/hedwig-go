package main

import (
	"os"

	"github.com/cloudchacho/hedwig-go/aws"
	"go.opentelemetry.io/otel/propagation"
)

const (
	awsQueueName = "DEV-MYAPP"
)

func awsBackendSettings() aws.Settings {
	return aws.Settings{
		AWSAccessKey:    os.Getenv("AWS_ACCESS_KEY"),
		AWSAccountID:    os.Getenv("AWS_ACCOUNT_ID"),
		AWSRegion:       os.Getenv("AWS_REGION"),
		AWSSecretKey:    os.Getenv("AWS_SECRET_KEY"),
		AWSSessionToken: os.Getenv("AWS_SESSION_TOKEN"),
		QueueName:       awsQueueName,
	}
}

func awsBackend() *aws.Backend {
	return aws.NewBackend(awsBackendSettings(), nil)
}

func awsPropagator() propagation.TraceContext {
	return propagation.TraceContext{}
}
