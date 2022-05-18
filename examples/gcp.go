package main

import (
	"os"

	gcloudPropagator "github.com/GoogleCloudPlatform/opentelemetry-operations-go/propagator"
	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/gcp"
)

const (
	gcpQueueName = "dev-myapp"
)

func gcpBackendSettings() gcp.Settings {
	return gcp.Settings{
		GoogleCloudProject: os.Getenv("GOOGLE_CLOUD_PROJECT"),
		QueueName:          gcpQueueName,
		Subscriptions:      []string{"dev-user-created-v1"},
	}
}

func gcpBackend(logger hedwig.Logger) *gcp.Backend {
	return gcp.NewBackend(gcpBackendSettings(), logger)
}

func gcpPropagator() gcloudPropagator.CloudTraceFormatPropagator {
	return gcloudPropagator.New()
}
