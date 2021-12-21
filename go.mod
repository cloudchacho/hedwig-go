module github.com/cloudchacho/hedwig-go

go 1.16

retract (
    v1.0.4 // this version doesn't exist in this repo, but goproxy thinks it does?
    v1.0.5 // Contains retractions only.
)

require (
	cloud.google.com/go/pubsub v1.11.0
	github.com/Masterminds/semver v1.5.0
	github.com/aws/aws-sdk-go v1.38.62
	github.com/golang/protobuf v1.5.2
	github.com/pkg/errors v0.9.1
	github.com/santhosh-tekuri/jsonschema/v3 v3.0.1
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/otel v0.20.0
	go.opentelemetry.io/otel/sdk v0.20.0
	go.opentelemetry.io/otel/trace v0.20.0
	golang.org/x/oauth2 v0.0.0-20210514164344-f6687ab2804c
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/api v0.48.0
	google.golang.org/grpc v1.38.0
	google.golang.org/protobuf v1.26.0
)
