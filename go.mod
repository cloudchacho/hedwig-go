module github.com/cloudchacho/hedwig-go

go 1.18

retract (
	v1.0.5 // Contains retractions only.
	v1.0.4 // this version doesn't exist in this repo, but goproxy thinks it does?
)

require (
	cloud.google.com/go/pubsub v1.25.1
	github.com/Masterminds/semver v1.5.0
	github.com/aws/aws-sdk-go v1.43.18
	github.com/google/uuid v1.3.0
	github.com/pkg/errors v0.9.1
	github.com/santhosh-tekuri/jsonschema/v3 v3.1.0
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/otel v1.4.1
	go.opentelemetry.io/otel/sdk v1.4.1
	go.opentelemetry.io/otel/trace v1.4.1
	golang.org/x/oauth2 v0.0.0-20220822191816-0ebed06d0094
	golang.org/x/sync v0.0.0-20220819030929-7fc1605a5dde
	google.golang.org/api v0.94.0
	google.golang.org/protobuf v1.28.1
)

require (
	cloud.google.com/go/compute v1.9.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/googleapis/gax-go/v2 v2.5.1 // indirect
	golang.org/x/net v0.0.0-20220822230855-b0a4917ee28c // indirect
	golang.org/x/sys v0.0.0-20220823224334-20c2bfdbfe24 // indirect
	google.golang.org/grpc v1.49.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)
