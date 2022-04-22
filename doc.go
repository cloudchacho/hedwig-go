package hedwig

/*

Hedwig is an inter-service communication bus that works on AWS and GCP, while keeping things pretty simple and straight
forward.

It allows validation of the message payloads before they are sent, helping to catch cross-component incompatibilities
early.

Hedwig allows separation of concerns between consumers and publishers so your services are loosely coupled, and the
contract is enforced by the message payload validation. Hedwig may also be used to build asynchronous APIs.

For intra-service messaging, see [Taskhawk](https://github.com/cloudchacho/taskhawk-go).

To learn more, [read the docs](https://cloudchacho.github.io/hedwig).

Provisioning

Hedwig works on SQS and SNS as backing queues. Before you can publish/consume messages, you need to provision the
required infra. This may be done manually, or, preferably, using Terraform. Hedwig provides tools to make infra
configuration easier: see [Terraform Google](https://github.com/cloudchacho/terraform-google-hedwig) for further
details.

Initialization

Create a protobuf schema and save as ``schema.proto``:

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

Clone hedwig Options definition file and compile your schema:

	git clone github.com/cloudchacho/hedwig /usr/local/lib/protobuf/include/hedwig/
	protoc -I/usr/local/lib/protobuf/include -I. --go_out=. schema.proto

In publisher application, initialize the publisher:

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

And finally, send a message:

    headers := map[string]string{}
    msg, err := hedwig.NewMessage("email.send", "1.0", headers, data, "myapp")
    if err != nil {
        return err
    }
    err := publisher.Publish(context.Background(), msg)

If you want to include a custom headers with the message (for example, you can include a request_id field
for cross-application tracing), you can pass it in additional parameter headers.

In consumer application, define your callback, which are simple functions that accept a context and `*hedwig.Message`.

    // Handler
    func HandleSendEmail(ctx context.Context, msg *hedwig.Message) error {
        to := msg.data.(*SendEmailV1).GetTo()
		// actually send email
    }

You can access the data map using message.data as well as custom headers using message.Metadata.Headers
and other metadata fields as described in the struct definition.

And start the consumer:

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

This is a blocking function.

For more complete code, see examples.

Schema

A schema defines the constraints of the data that your message can contain. For example, you may want to define that
your messages are valid JSON values with a specific key. Hedwig provides schema validation so that your application
doesn't have to think about whether messages are valid.

Note that the schema file only contains definitions for major versions. This is by design since minor version MUST be
backwards compatible.

Currently, protobuf and json-schema are supported out of the box, but you can plug in your own validator.

*/
