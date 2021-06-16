module github.com/cloudchacho/hedwig-go/protobuf

go 1.16

replace github.com/cloudchacho/hedwig-go v0.0.0 => ../

replace github.com/cloudchacho/hedwig-go/protobuf/internal v0.0.0 => ./internal

require (
	github.com/Masterminds/semver v1.5.0
	github.com/cloudchacho/hedwig-go v0.0.0
	github.com/cloudchacho/hedwig-go/protobuf/internal v0.0.0
	github.com/golang/protobuf v1.5.2
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	google.golang.org/protobuf v1.26.0
)
