# Hedwig Library for Go

[![Build Status](https://github.com/cloudchacho/hedwig-go/checks)](https://github.com/cloudchacho/hedwig-go/actions/workflows/gotest.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/cloudchacho/hedwig-go)](https://goreportcard.com/report/github.com/cloudchacho/hedwig-go)
[![Godoc](https://godoc.org/github.com/cloudchacho/hedwig-go?status.svg)](http://godoc.org/github.com/cloudchacho/hedwig-go)
[![codecov](https://codecov.io/gh/cloudchacho/hedwig-go/branch/main/graph/badge.svg?token=H6VWFF04JD)](https://codecov.io/gh/cloudchacho/hedwig-go)

Hedwig is an inter-service communication bus that works on AWS and GCP, while keeping things pretty simple and straight
forward.

It allows validation of the message payloads before they are sent, helping to catch cross-component incompatibilities
early.

Hedwig allows separation of concerns between consumers and publishers so your services are loosely coupled, and the
contract is enforced by the message payload validation. Hedwig may also be used to build asynchronous APIs.

For intra-service messaging, see [Taskhawk](https://github.com/cloudchacho/taskhawk-go).

To learn more, [read the docs](https://cloudchacho.github.io/hedwig).

## Fan Out

Hedwig utilizes SNS for fan-out configuration. A publisher publishes messages on a topic. This message may be received by zero or more consumers. The publisher need not be aware of the consuming application. There are a variety of messages that may be published as such, but they generally fall into two buckets:

- **Asynchronous API Requests**: Hedwig may be used to call APIs asynchronously. The contract is enforced by your infra-structure by connecting SNS topics to SQS queues, and payload is validated using the schema you define. Response is a delivered using a separate message if required.
- **Notifications**: The most common use case is to notify other services/apps that may be interested in events. For example, your User Management app can publish a user.created message notification to all your apps. As publishers and consumers are loosely coupled, this separation of concerns is very effective in ensuring a stable eco-system.

## Provisioning

Hedwig works on SQS and SNS as backing queues. Before you can publish/consume messages, you need to provision the
required infra. This may be done manually, or, preferably, using Terraform. Hedwig provides tools to make infra
configuration easier: see [Terraform](https://github.com/cloudchacho/hedwig-terraform) and
[Hedwig Terraform Generator](https://github.com/cloudchacho/hedwig-terraform-generator) for further details.


## Quick Start

First, install the library:

```bash
go get github.com/cloudchacho/hedwig-go
```

Create a protobuf schema and save as ``schema.proto``:

```protobuf
syntax = "proto2";

package main;

import "hedwig/protobuf/options.proto";

option go_package = "example.com/hedwig;main";

message SendEmailV1 {
    option (hedwig.message_options).major_version = 1;
    option (hedwig.message_options).minor_version = 0;
    option (hedwig.message_options).message_type = "email.send";

    string to = 1;
    string message = 1;
}
```

Clone hedwig Options definition file and compile your schema:
```shell
git clone github.com/cloudchacho/hedwig /usr/local/lib/protobuf/include/hedwig/
protoc -I/usr/local/lib/protobuf/include -I. --go_out=. schema.proto
```

In publisher application, initialize the publisher:

```go
    settings := aws.Settings {
        AWSAccessKey:    <YOUR AWS KEY>,
        AWSAccountID:    <YOUR AWS ACCOUNT ID>,
        AWSRegion:       <YOUR AWS REGION>,
        AWSSecretKey:    <YOUR AWS SECRET KEY>,
        AWSSessionToken: <YOUR AWS SESSION TOKEN>, 
    }
    backend := aws.NewBackend(settings, nil)
	encoderDecoder := protobuf.NewMessageEncoderDecoder([]proto.Message{&SendEmailV1{}})
    routing := map[hedwig.MessageRouteKey]string{
        {
            MessageType:    "email.send",
            MessageMajorVersion: 1,
        }: "send_email",
    }
    publisher := hedwig.NewPublisher(backend, encoderDecoder, encoderDecoder, routing)
```

And finally, send a message:

```go
    headers := map[string]string{}
    msg, err := hedwig.NewMessage("email.send", "1.0", headers, data, "myapp")
    if err != nil {
        return err
    }
    err := publisher.Publish(context.Background(), msg)
```

In consumer application, define your callback:

```go
    // Handler
    func HandleSendEmail(ctx context.Context, msg *hedwig.Message) error {
        to := msg.data.(*SendEmailV1).GetTo()
		// actually send email
    }
```

And start the consumer:
```go
    settings := aws.Settings {
        AWSAccessKey:    <YOUR AWS KEY>,
        AWSAccountID:    <YOUR AWS ACCOUNT ID>,
        AWSRegion:       <YOUR AWS REGION>,
        AWSSecretKey:    <YOUR AWS SECRET KEY>,
        AWSSessionToken: <YOUR AWS SESSION TOKEN>, 
    }
    backend := aws.NewBackend(settings, nil)
	encoderDecoder := protobuf.NewMessageEncoderDecoder([]proto.Message{&SendEmailV1{}})
    registry := hedwig.CallbackRegistry{{"email.send", 1}: HandleSendEmail}
    consumer := hedwig.NewConsumer(backend, encoderDecoder, nil, registry)
    err := consumer.ListenForMessages(context.Background(), hedwig.ListenRequest{})
```

For more complete code, see [examples](examples).

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

### v0.8

- Cleaned up interfaces to be more idiomatic Go

### v0.7

- Initial version 
