/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

/*

Hedwig is a inter-service communication bus that works on AWS SQS/SNS, while keeping things pretty simple and
straight forward. It uses JSON schema draft v4 (http://json-schema.org/specification-links.html#draft-4) for schema validation so all incoming
and outgoing messages are validated against pre-defined schema.

Hedwig allows separation of concerns between consumers and publishers so your services are loosely coupled, and the
contract is enforced by the schema validation. Hedwig may also be used to build asynchronous APIs.

For intra-service messaging, see Taskhawk (https://github.com/Automatic/taskhawk-go).

Provisioning

Hedwig works on SQS and SNS as backing queues. Before you can publish/consume messages, you need to provision the
required infra. This may be done manually, or, preferably, using Terraform. Hedwig provides tools to make infra
configuration easier: see Terraform (https://github.com/Automatic/hedwig-terraform) and
Hedwig Terraform Generator (https://github.com/Automatic/hedwig-terraform-generator) for further details.

Initialization

Define a few required settings. Please see the settings struct for the additional optional parameters.

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

Schema

The schema file must be a JSON-Schema draft v4 schema. There’s a few more restrictions in addition to being a valid schema:

  - There must be a top-level key called `schemas`. The value must be an object.
  - `schemas`: The keys of this object must be message types. The value must be an object.
  - `schemas/<message_type>`: The keys of this object must be major version patterns for this message type. The value must be an object.
  - `schemas/<message_type>/<major_version>.*`: This object must represent the data schema for given message type, and major version. Any minor version updates must be applied in-place, and must be non-breaking per semantic versioning.
  - Optionally, a key `x-versions` may be used to list full versions under a major version.

Note that the schema file only contains definitions for major versions. This is by design since minor version MUST be

Optionally, a key `x-versions` may be used to list full versions under a major version.

For an example, see test hedwig schema: https://github.com/cloudchacho/hedwig-go/blob/master/hedwig/schema.json.

Callbacks

Callbacks are simple functions that accept a context and a hedwig.Message struct.

    func HandleSendEmail(ctx context.Context, message *hedwig.Message) error {
        // Send email
    }

You can access the data map using message.data as well as custom headers using message.Metadata.Headers
and other metadata fields as described in the struct definition.

Publisher

Assuming the publisher has already been initialized, You can publish messages like so:

    headers := map[string]string{}
    msg, err := hedwig.NewMessage(settings, "email.send", "1.0", headers, data)
    if err != nil {
        return err
    }
    publisher.Publish(ctx, msg)

If you want to include a custom headers with the message (for example, you can include a request_id field
for cross-application tracing), you can pass it in additional parameter headers.

Consumer

A consumer for SQS based workers can be started as following:

    consumer := hedwig.NewQueueConsumer(sessionCache, settings)
    consumer.ListenForMessages(ctx, &hedwig.ListenRequest{...})

This is a blocking function.

A consumer for Lambda based workers can be started as following:

    consumer = hedwig.NewLambdaConsumer(sessionCache, settings)
    consumer.HandleLambdaEvent(ctx, snsEvent)

where snsEvent is the event provided by AWS to your Lambda
function as described in AWS documentation: https://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-sns.
The lambda event handler can also be passed into the AWS Lambda SDK as follows:

    lambda.Start(consumer.HandleLambdaEvent)
*/
