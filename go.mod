module github.com/cloudchacho/hedwig-go

go 1.18

retract (
	v1.0.5 // Contains retractions only.
	v1.0.4 // this version doesn't exist in this repo, but goproxy thinks it does?
)

require (
	cloud.google.com/go/pubsub v1.19.0
	github.com/Masterminds/semver v1.5.0
	github.com/aws/aws-sdk-go v1.43.18
	github.com/pkg/errors v0.9.1
	github.com/santhosh-tekuri/jsonschema/v3 v3.1.0
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/otel v1.4.1
	go.opentelemetry.io/otel/sdk v1.4.1
	go.opentelemetry.io/otel/trace v1.4.1
	golang.org/x/oauth2 v0.0.0-20220309155454-6242fa91716a
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/api v0.73.0
	google.golang.org/protobuf v1.27.1
)

require (
	cloud.google.com/go/iam v0.3.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	golang.org/x/sys v0.0.0-20220315180522-27bbf83dae87 // indirect
	google.golang.org/genproto v0.0.0-20220314164441-57ef72a4c106 // indirect
)
