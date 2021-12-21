module examples

go 1.16

replace github.com/cloudchacho/hedwig-go v0.0.0 => ../

require (
	github.com/GoogleCloudPlatform/opentelemetry-operations-go v1.0.0-RC2.0.20210816152642-29dd0bfc39f0
	github.com/cloudchacho/hedwig-go v0.0.0
	go.opentelemetry.io/otel v0.20.0
	go.opentelemetry.io/otel/sdk v0.20.0
	go.opentelemetry.io/otel/trace v0.20.0
	google.golang.org/protobuf v1.26.0
)
