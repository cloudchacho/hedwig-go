module examples

go 1.16

replace github.com/cloudchacho/hedwig-go v0.0.0 => ../

require (
	github.com/GoogleCloudPlatform/opentelemetry-operations-go v1.3.0
	github.com/cloudchacho/hedwig-go v0.0.0
	github.com/rs/zerolog v1.26.1
	github.com/sirupsen/logrus v1.8.1
	go.opentelemetry.io/otel v1.4.1
	go.opentelemetry.io/otel/sdk v1.4.1
	go.opentelemetry.io/otel/trace v1.4.1
	go.uber.org/zap v1.21.0
	google.golang.org/protobuf v1.27.1
)
