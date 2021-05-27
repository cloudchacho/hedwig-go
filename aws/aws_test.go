package aws

import (
	"context"
	"encoding/base64"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudchacho/hedwig-go"
)

type fakeHedwigDataField struct {
	VehicleID string `json:"vehicle_id"`
}

type fakeLog struct {
	level   string
	err     error
	message string
	fields  hedwig.LoggingFields
}

type fakeLogger struct {
	lock sync.Mutex
	logs []fakeLog
}

func (f *fakeLogger) Error(err error, message string, fields hedwig.LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"error", err, message, fields})
}

func (f *fakeLogger) Warn(err error, message string, fields hedwig.LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"warn", err, message, fields})
}

func (f *fakeLogger) Info(message string, fields hedwig.LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"info", nil, message, fields})
}

func (f *fakeLogger) Debug(message string, fields hedwig.LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"debug", nil, message, fields})
}

type fakeValidator struct {
	mock.Mock
}

type fakeConsumerCallback struct {
	mock.Mock
}

func (fc *fakeConsumerCallback) Callback(ctx context.Context, payload []byte, attributes map[string]string, providerMetadata interface{}) {
	fc.Called(ctx, payload, attributes, providerMetadata)
}

func (f *fakeValidator) Serialize(message *hedwig.Message) ([]byte, map[string]string, error) {
	args := f.Called(message)
	return args.Get(0).([]byte), args.Get(1).(map[string]string), args.Error(2)
}

func (f *fakeValidator) Deserialize(messagePayload []byte, attributes map[string]string, providerMetadata interface{}) (*hedwig.Message, error) {
	args := f.Called(messagePayload, attributes, providerMetadata)
	return args.Get(0).(*hedwig.Message), args.Error(1)
}

type fakeSQS struct {
	mock.Mock
	// fake interface here
	sqsiface.SQSAPI
}

func (fs *fakeSQS) SendMessageWithContext(ctx aws.Context, in *sqs.SendMessageInput, opts ...request.Option) (*sqs.SendMessageOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.SendMessageOutput), args.Error(1)
}

func (fs *fakeSQS) GetQueueUrlWithContext(ctx aws.Context, in *sqs.GetQueueUrlInput, opts ...request.Option) (*sqs.GetQueueUrlOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.GetQueueUrlOutput), args.Error(1)
}

func (fs *fakeSQS) ReceiveMessageWithContext(ctx aws.Context, in *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.ReceiveMessageOutput), args.Error(1)
}

func (fs *fakeSQS) DeleteMessageWithContext(ctx aws.Context, in *sqs.DeleteMessageInput, opts ...request.Option) (*sqs.DeleteMessageOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.DeleteMessageOutput), args.Error(1)
}

type fakeSNS struct {
	mock.Mock
	// fake interface here
	snsiface.SNSAPI
}

func (fs *fakeSNS) PublishWithContext(ctx aws.Context, in *sns.PublishInput, opts ...request.Option) (*sns.PublishOutput, error) {
	args := fs.Called(ctx, in)
	return args.Get(0).(*sns.PublishOutput), args.Error(1)
}

func (s *BackendTestSuite) TestReceive() {
	ctx, cancel := context.WithCancel(context.Background())
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	receiptHandle := "foobar"
	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	firstReceiveTime, err := time.Parse(time.RFC3339Nano, "2011-01-19T22:15:10.456000000-07:00")
	s.Require().NoError(err)
	sentTime, err := time.Parse(time.RFC3339Nano, "2011-01-19T22:15:10.123000000-07:00")
	s.Require().NoError(err)
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageId := "123"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": &sqs.MessageAttributeValue{StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageId),
	}
	body2 := `vbI9vCDijJg=`
	messageId2 := "456"
	sqsMessage2 := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo":             &sqs.MessageAttributeValue{StringValue: aws.String("bar")},
			"hedwig_encoding": &sqs.MessageAttributeValue{StringValue: aws.String("base64")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body2),
		MessageId: aws.String(messageId2),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage, &sqsMessage2},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.cancelCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once()
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.cancelCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	providerMetadata := AWSMetadata{
		ReceiptHandle:    receiptHandle,
		FirstReceiveTime: firstReceiveTime.UTC(),
		SentTime:         sentTime.UTC(),
		ReceiveCount:     receiveCount,
	}
	s.fakeConsumerCallback.On("Callback", mock.AnythingOfType("*context.cancelCtx"), payload, attributes, providerMetadata).
		Return().
		Once()
	payload2 := []byte("\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98")
	attributes2 := map[string]string{
		"foo":             "bar",
		"hedwig_encoding": "base64",
	}
	s.fakeConsumerCallback.On("Callback", mock.AnythingOfType("*context.cancelCtx"), payload2, attributes2, providerMetadata).
		Return().
		Once().
		// force method to return after just one loop
		After(time.Millisecond * 11)

	ch := make(chan bool)
	go func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.EqualError(err, "context canceled")
		ch <- true
		close(ch)
	}()
	time.Sleep(time.Millisecond * 10)
	cancel()

	// wait for co-routine to finish
	<-ch

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestReceiveFailedNonUTF8Decoding() {
	ctx, cancel := context.WithCancel(context.Background())
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	receiptHandle := "foobar"
	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `foobar`
	messageId := "123"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo":             &sqs.MessageAttributeValue{StringValue: aws.String("bar")},
			"hedwig_encoding": &sqs.MessageAttributeValue{StringValue: aws.String("base64")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageId),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{Messages: []*sqs.Message{&sqsMessage}}
	s.fakeSQS.On("ReceiveMessageWithContext", ctx, receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once()
	s.fakeSQS.On("ReceiveMessageWithContext", ctx, receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	ch := make(chan bool)
	go func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.EqualError(err, "context canceled")
		ch <- true
		close(ch)
	}()
	time.Sleep(time.Millisecond * 10)
	cancel()

	// wait for co-routine to finish
	<-ch

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Invalid message payload - couldn't decode using base64")
	s.Error(s.logger.logs[0].err)
}

func (s *BackendTestSuite) TestReceiveNoMessages() {
	ctx, cancel := context.WithCancel(context.Background())
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	s.fakeSQS.On("ReceiveMessageWithContext", ctx, receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	ch := make(chan bool)
	go func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.EqualError(err, "context canceled")
		ch <- true
		close(ch)
	}()
	time.Sleep(time.Millisecond * 1)
	cancel()

	// wait for co-routine to finish
	<-ch

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestReceiveError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	s.fakeSQS.On("ReceiveMessageWithContext", ctx, receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, errors.New("no internet"))

	ch := make(chan bool)
	go func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.EqualError(err, "failed to receive SQS message: no internet")
		ch <- true
		close(ch)
	}()

	// wait for co-routine to finish
	<-ch

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestReceiveGetQueueError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).
		Return((*sqs.GetQueueUrlOutput)(nil), errors.New("no internet"))

	err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
	s.EqualError(err, "failed to get SQS Queue URL: no internet")

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestReceiveShutdown() {
	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(time.Second*1))
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	ch := make(chan bool)
	go func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.EqualError(err, "context shutting down")
		ch <- true
		close(ch)
	}()

	// wait for co-routine to finish
	<-ch

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestReceiveMissingAttributes() {
	ctx, cancel := context.WithCancel(context.Background())
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiptHandle := "123"
	body := `{"vehicle_id": "C_123"}`
	messageId := "123"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": &sqs.MessageAttributeValue{StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String(""),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String(""),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(""),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageId),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.cancelCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once()
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.cancelCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	providerMetadata := AWSMetadata{
		ReceiptHandle:    receiptHandle,
		FirstReceiveTime: time.Time{},
		SentTime:         time.Time{},
		ReceiveCount:     -1,
	}
	s.fakeConsumerCallback.On("Callback", mock.AnythingOfType("*context.cancelCtx"), payload, attributes, providerMetadata).
		Return().
		Once()

	ch := make(chan bool)
	go func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.EqualError(err, "context canceled")
		ch <- true
		close(ch)
	}()
	time.Sleep(time.Millisecond * 1)
	cancel()

	// wait for co-routine to finish
	<-ch

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestPublish() {
	ctx := context.Background()

	msgTopic := "dev-myapp"
	expectedTopic := s.backend.getSNSTopic(msgTopic)

	attributes := map[string]*sns.MessageAttributeValue{
		"foo": {
			DataType:    aws.String("String"),
			StringValue: aws.String("bar"),
		},
	}

	expectedSnsInput := &sns.PublishInput{
		TopicArn:          &expectedTopic,
		Message:           aws.String(string(s.payload)),
		MessageAttributes: attributes,
	}
	output := &sns.PublishOutput{
		MessageId: aws.String("123"),
	}

	s.fakeSNS.On("PublishWithContext", ctx, expectedSnsInput, mock.Anything).
		Return(output, nil)

	messageId, err := s.backend.Publish(ctx, s.message, s.payload, s.attributes, msgTopic)
	s.NoError(err)
	s.Equal(messageId, "123")

	s.fakeSNS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestPublishInvalidCharacters() {
	ctx := context.Background()

	msgTopic := "dev-myapp"
	expectedTopic := s.backend.getSNSTopic(msgTopic)

	attributes := map[string]*sns.MessageAttributeValue{
		"foo": {
			DataType:    aws.String("String"),
			StringValue: aws.String("bar"),
		},
		"hedwig_encoding": {
			DataType:    aws.String("String"),
			StringValue: aws.String("base64"),
		},
	}

	invalidPayload := []byte("\x19")

	expectedSnsInput := &sns.PublishInput{
		TopicArn:          &expectedTopic,
		Message:           aws.String(base64.StdEncoding.EncodeToString(invalidPayload)),
		MessageAttributes: attributes,
	}
	output := &sns.PublishOutput{
		MessageId: aws.String("123"),
	}

	s.fakeSNS.On("PublishWithContext", ctx, expectedSnsInput, mock.Anything).
		Return(output, nil)

	messageId, err := s.backend.Publish(ctx, s.message, invalidPayload, s.attributes, msgTopic)
	s.NoError(err)
	s.Equal(messageId, "123")

	s.fakeSNS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestPublishFailure() {
	ctx := context.Background()

	msgTopic := "dev-myapp"
	expectedTopic := s.backend.getSNSTopic(msgTopic)

	attributes := map[string]*sns.MessageAttributeValue{
		"foo": {
			DataType:    aws.String("String"),
			StringValue: aws.String("bar"),
		},
	}

	expectedSnsInput := &sns.PublishInput{
		TopicArn:          &expectedTopic,
		Message:           aws.String(string(s.payload)),
		MessageAttributes: attributes,
	}

	s.fakeSNS.On("PublishWithContext", ctx, expectedSnsInput, mock.Anything).
		Return((*sns.PublishOutput)(nil), errors.New("failed"))

	_, err := s.backend.Publish(ctx, s.message, s.payload, s.attributes, msgTopic)
	s.EqualError(err, "Failed to publish message to SNS: failed")

	s.fakeSNS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestAck() {
	ctx := context.Background()

	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	receiptHandle := "foobar"

	deleteInput := &sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: aws.String(receiptHandle),
	}
	deleteOutput := &sqs.DeleteMessageOutput{}

	s.fakeSQS.On("DeleteMessageWithContext", ctx, deleteInput, mock.Anything).
		Return(deleteOutput, nil)

	err := s.backend.AckMessage(ctx, AWSMetadata{ReceiptHandle: receiptHandle})
	s.NoError(err)

	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestAckError() {
	ctx := context.Background()

	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	receiptHandle := "foobar"

	deleteInput := &sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: aws.String(receiptHandle),
	}

	s.fakeSQS.On("DeleteMessageWithContext", ctx, deleteInput, mock.Anything).
		Return((*sqs.DeleteMessageOutput)(nil), errors.New("failed to ack"))

	err := s.backend.AckMessage(ctx, AWSMetadata{ReceiptHandle: receiptHandle})
	s.EqualError(err, "failed to ack")

	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestAckGetQueueError() {
	ctx := context.Background()

	queueName := "HEDWIG-DEV-MYAPP"
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).
		Return((*sqs.GetQueueUrlOutput)(nil), errors.New("no internet"))

	receiptHandle := "foobar"

	err := s.backend.AckMessage(ctx, AWSMetadata{ReceiptHandle: receiptHandle})
	s.EqualError(err, "failed to get SQS Queue URL: no internet")

	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestNack() {
	ctx := context.Background()

	receiptHandle := "foobar"

	err := s.backend.NackMessage(ctx, AWSMetadata{ReceiptHandle: receiptHandle})
	s.NoError(err)

	// no calls expected
	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestNew() {
	assert.NotNil(s.T(), s.backend)
}

type BackendTestSuite struct {
	suite.Suite
	backend              *awsBackend
	settings             *hedwig.Settings
	fakeSQS              *fakeSQS
	fakeSNS              *fakeSNS
	message              *hedwig.Message
	payload              []byte
	attributes           map[string]string
	validator            *fakeValidator
	fakeConsumerCallback *fakeConsumerCallback
	logger               *fakeLogger
}

func (s *BackendTestSuite) SetupTest() {
	logger := &fakeLogger{}
	settings := &hedwig.Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "DEV-MYAPP",
		MessageRouting: map[hedwig.MessageTypeMajorVersion]string{
			{
				MessageType:  "user-created",
				MajorVersion: 1,
			}: "dev-user-created-v1",
		},
		GetLogger: func(ctx context.Context) hedwig.ILogger {
			return logger
		},
		ShutdownTimeout: time.Second * 10,
	}
	fakeSQS := &fakeSQS{}
	fakeSNS := &fakeSNS{}
	fakeMessageCallback := &fakeConsumerCallback{}
	message, err := hedwig.NewMessage(settings, "user-created", "1.0", map[string]string{"foo": "bar"}, &fakeHedwigDataField{})
	require.NoError(s.T(), err)

	validator := &fakeValidator{}

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{"foo": "bar"}

	s.backend = NewAWSBackend(settings, NewAWSSessionsCache()).(*awsBackend)
	s.backend.sqs = fakeSQS
	s.backend.sns = fakeSNS
	s.settings = settings
	s.fakeSQS = fakeSQS
	s.fakeSNS = fakeSNS
	s.message = message
	s.validator = validator
	s.payload = payload
	s.attributes = attributes
	s.fakeConsumerCallback = fakeMessageCallback
	s.logger = logger
}

func TestBackendTestSuite(t *testing.T) {
	suite.Run(t, &BackendTestSuite{})
}
