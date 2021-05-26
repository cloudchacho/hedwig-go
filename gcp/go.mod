module github.com/cloudchacho/hedwig-go/gcp

go 1.16

replace github.com/cloudchacho/hedwig-go v0.0.0 => ../

require (
	cloud.google.com/go/pubsub v1.10.3
	github.com/cloudchacho/hedwig-go v0.0.0
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/api v0.45.0
)
