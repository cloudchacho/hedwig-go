# Hedwig Library for Go

[![Build Status](https://github.com/cloudchacho/hedwig-go/checks)](https://github.com/github/cloudchacho/hedwig-go/workflows/gotest.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/cloudchacho/hedwig-go)](https://goreportcard.com/report/github.com/cloudchacho/hedwig-go)
[![Godoc](https://godoc.org/github.com/cloudchacho/hedwig-go?status.svg)](http://godoc.org/github.com/cloudchacho/hedwig-go)
[![Coverage](https://img.shields.io/coveralls/cloudchacho/hedwig-go/master.svg?style=flat-square)](https://coveralls.io/r/cloudchacho/hedwig-go)


Hedwig is a inter-service communication bus that works on AWS SQS/SNS, while keeping things pretty simple and
straight forward. It uses [JSON schema](http://json-schema.org/) [draft v4](http://json-schema.org/specification-links.html#draft-4) for schema validation so all incoming
and outgoing messages are validated against pre-defined schema.

Hedwig allows separation of concerns between consumers and publishers so your services are loosely coupled, and the
contract is enforced by the schema validation. Hedwig may also be used to build asynchronous APIs.

For intra-service messaging, see [Taskhawk](https://github.com/Automatic/taskhawk-go).

## Fan Out

Hedwig utilizes SNS for fan-out configuration. A publisher publishes messages on a topic. This message may be received by zero or more consumers. The publisher need not be aware of the consuming application. There are a variety of messages that may be published as such, but they generally fall into two buckets:

- **Asynchronous API Requests**: Hedwig may be used to call APIs asynchronously. The contract is enforced by your infra-structure by connecting SNS topics to SQS queues, and payload is validated using the schema you define. Response is a delivered using a separate message if required.
- **Notifications**: The most common use case is to notify other services/apps that may be interested in events. For example, your User Management app can publish a user.created message notification to all your apps. As publishers and consumers are loosely coupled, this separation of concerns is very effective in ensuring a stable eco-system.

## Provisioning

Hedwig works on SQS and SNS as backing queues. Before you can publish/consume messages, you need to provision the
required infra. This may be done manually, or, preferably, using Terraform. Hedwig provides tools to make infra
configuration easier: see [Terraform](https://github.com/Automatic/hedwig-terraform) and
[Hedwig Terraform Generator](https://github.com/Automatic/hedwig-terraform-generator) for further details.


## Quick Start

First, install the library:

```bash
go get github.com/cloudchacho/hedwig-go
```

Create a JSON-schema and save as ``schema.json``:

```json

    {
        "id": "https://hedwig.automatic.com/schema#",
        "$schema": "http://json-schema.org/draft-04/schema",
        "schemas": {
            "email.send": {
                "1.*": {
                    "description": "Request to send email",
                    "type": "object",
                    "required": [
                        "to",
                        "subject"
                    ],
                    "properties": {
                        "to": {
                            "type": "string",
                            "pattern": "^\\S+@\\S+$"
                        },
                        "subject": {
                            "type": "string",
                            "minLength": 2
                        }
                    }
                }
            }
        }
    }
```

Next, set up a few configuration settings:

```go
    validator, err := hedwig.NewMessageValidator("schema.json")
    if err != nil {
        panic("Failed to create validator")
    }
    settings := &hedwig.Settings{
        AWSAccessKey:              <YOUR AWS KEY>,
        AWSAccountID:              <YOUR AWS ACCOUNT ID>,
        AWSRegion:                 <YOUR AWS REGION>,
        AWSSecretKey:              <YOUR AWS SECRET KEY>,
        AWSSessionToken:           <YOUR AWS SESSION TOKEN>,
        CallbackRegistry:          hedwig.NewCallbackRegistry(),
        Publisher:                 "MYAPP",
        QueueName:                 "DEV-MYAPP",
        MessageRouting:            map[hedwig.MessageRouteKey]string{
            hedwig.MessageRouteKey{
                MessageType:    "email.send",
    		        MessageMajorVersion: 1,
    	      }: "send_email",
        },
        Validator:                 validator,
    }
```

These configuration settings will be passed into the library for initialization.

Next define the models associated with the schemas. These models should have factory
functions as well.

```go
    type SendEmail struct {
        Subject string `json:"subject"`
        To      string `json:"to"`
    }

    // Factory that returns empty struct
    func NewSendEmailData() interface{} { return new(SendEmail) }
```

Then, simply define your topic handler and register the handler:

```go
    // Handler
    func HandleSendEmail(ctx context.Context, msg *hedwig.Message) error {
        // Send email
    }

    // Register handler
    cbk := CallbackKey{
        MessageType:    "email.send",
        MessageMajorVersion: 1,
    }
    settings.CallbackRegistry.RegisterCallback(cbk, HandleSendEmail, NewSendEmailData)
```

Initialize the publisher

```go
    sessionCache := hedwig.NewAWSSessionsCache()
    publisher := hedwig.NewPublisher(sessionCache, settings)
```

And finally, send a message:

```go
    headers := map[string]string{}
    msg, err := hedwig.NewMessage(settings, "email.send", "1.0", headers, data)
    if err != nil {
        return err
    }
    publisher.Publish(context.Background(), msg)
```


## Development

### Prerequisites

Install go1.11.x

### Getting Started

```bash

$ cd ${GOPATH}/src/github.com/cloudchacho/hedwig-go
$ go build
```

### Running Tests

```bash

$ make test
```

## Getting Help

We use GitHub issues for tracking bugs and feature requests.

* If it turns out that you may have found a bug, please [open an issue](https://github.com/cloudchacho/hedwig-go/issues/new>)

## Release notes

**Current version: v1.0.1-dev**

### v1.0.0

  - Initial version
