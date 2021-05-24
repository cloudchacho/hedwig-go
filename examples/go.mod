module examples

go 1.16

replace github.com/cloudchacho/hedwig-go v0.0.0 => ../

replace github.com/cloudchacho/hedwig-go/jsonschema v0.0.0 => ../jsonschema

replace github.com/cloudchacho/hedwig-go/aws v0.0.0 => ../aws

require (
	github.com/cloudchacho/hedwig-go v0.0.0
	github.com/cloudchacho/hedwig-go/aws v0.0.0 // indirect
	github.com/cloudchacho/hedwig-go/jsonschema v0.0.0
)
