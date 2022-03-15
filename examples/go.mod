module examples

go 1.16

replace github.com/cloudchacho/hedwig-go v0.0.0 => ../

require (
	github.com/GoogleCloudPlatform/opentelemetry-operations-go v1.3.0
	github.com/cloudchacho/hedwig-go v0.0.0
	go.opentelemetry.io/otel v1.4.1
	go.opentelemetry.io/otel/sdk v1.4.1
	go.opentelemetry.io/otel/trace v1.4.1
	google.golang.org/protobuf v1.27.1
)
