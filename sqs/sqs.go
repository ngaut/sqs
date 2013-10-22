//
// goamz - Go packages to interact with the Amazon Web Services.
//
//   https://wiki.ubuntu.com/goamz
//
// Copyright (c) 2012 Memeo Inc.
//
// Written by Prudhvi Krishna Surapaneni <me@prudhvi.net>
//
package sqs

import (
	"encoding/xml"
	"fmt"
	"launchpad.net/goamz/aws"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"
)

// The SQS type encapsulates operations with specific SQS region
type SQS struct {
	aws.Auth
	aws.Region
	private byte // Reserve the right of using private data.
}

const debug = false

// New creates a new SQS handle
func New(auth aws.Auth, region aws.Region) *SQS {
	return &SQS{auth, region, 0}
}

// Queue type encapsulates operations on a SQS Queue
type Queue struct {
	*SQS
	Url string
}

// Response to a CreateQueue request.
//
// See http://goo.gl/sVUjF for more details
type CreateQueueResponse struct {
	QueueUrl string `xml:"CreateQueueResult>QueueUrl"`
	ResponseMetadata
}

// Response to a ListQueues request.
//
// See http://goo.gl/RPRWr for more details
type ListQueuesResponse struct {
	QueueUrl []string `xml:"ListQueuesResult>QueueUrl"`
	ResponseMetadata
}

// Response to a GetQueueUrl request.
//
// See http://goo.gl/hk7Iu for more details
type GetQueueUrlResponse struct {
	QueueUrl string `xml:"GetQueueUrlResult>QueueUrl"`
	ResponseMetadata
}

// Response to a ChangeMessageVisibility request.
//
// See http://goo.gl/EyJKF for more details
type ChangeMessageVisibilityResponse struct {
	ResponseMetadata
}

// See http://goo.gl/XTo0s for more details
type ResponseMetadata struct {
	RequestId string `xml:"ResponseMetadata>RequestId"`
}

// Error represents an error in an operation with SQS
type Error struct {
	StatusCode int    // HTTP Status Code (200, 403, ... )
	Code       string // SQS Error Code
	Message    string // The human-oriented error message
	RequestId  string `xml:"RequestID"`
}

func (err *Error) Error() string {
	if err.Code != "" {
		return err.Message
	}

	return fmt.Sprintf("%s (%s)", err.Message, err.Code)
}

// For now a single error inst is being exposed. In the future it may be useful
// to provide access to all of them, but rather than doing it as an array/slice,
// use a *next pointer, so that it's backward compatible and it continues to be
// easy to handle the first error, which is what most people will want.
type xmlErrors struct {
	RequestId string
	Errors    []Error `xml:"Errors>Error"`
}

// Attribute represents an instance of a SQS Queue Attribute.
type Attribute struct {
	Name  string
	Value string
}

// See http://goo.gl/pgffn for more details.
type ChangeMessageVisibilityBatchEntry struct {
	Id                string
	ReceiptHandle     string
	VisibilityTimeout int
}

// ReceiveMessageResponse holds the results of ReceiveMessage
type ReceiveMessageResponse struct {
	Messages []Message `xml:"ReceiveMessageResult>Message"`
	ResponseMetadata
}

type DeleteMessageResponse struct {
	ResponseMetadata
}

type SendMessageResponse struct {
	SendMessageResult
	ResponseMetadata
}

type SendMessageBatchResponse struct {
	SendMessageBatchResult
	ResponseMetadata
}

// SendMessageBatchResult holds the results of SendMessageBatch
type SendMessageBatchResult struct {
	Entries []SendMessageBatchResultEntry `xml:"SendMessageBatchResult>SendMessageBatchResultEntry"`
}

type SendMessageBatchResultEntry struct {
	MD5OfMessageBody string `xml:"MD5OfMessageBody"`
	MessageId        string `xml:"MessageId"`
	Id               string `xml:"Id"`
}

type SendMessageBatchRequestEntry struct {
	Id           string
	MessageBody  string
	DelaySeconds int
}

type SendMessageResult struct {
	MD5OfMessageBody string `xml:"SendMessageResult>MD5OfMessageBody"`
	MessageId        string `xml:"SendMessageResult>MessageId"`
}

// Represents an instance of a SQS Message
type Message struct {
	MessageId     string      `xml:"MessageId"`
	Body          string      `xml:"Body"`
	MD5OfBody     string      `xml:"MD5OfBody"`
	ReceiptHandle string      `xml:"ReceiptHandle"`
	Attribute     []Attribute `xml:"Attribute"`
}

type ChangeMessageVisibilityBatchResponse struct {
	Id []string `xml:"ChangeMessageVisibilityBatchResult>ChangeMessageVisibilityBatchResultEntry>Id"`
	ResponseMetadata
}

type GetQueueAttributesResponse struct {
	Attributes []Attribute `xml:"GetQueueAttributesResult>Attribute"`
	ResponseMetadata
}

type DeleteMessageBatchResult struct {
	Ids []string `xml:"DeleteMessageBatchResult>DeleteMessageBatchResultEntry>Id"`
}

type DeleteMessageBatchResponse struct {
	DeleteMessageBatchResult `xml:"DeleteMessageBatchResponse>DeleteMessageBatchResult"`
	ResponseMetadata
}

type DeleteMessageBatch struct {
	Id            string
	ReceiptHandle string
}

type AccountPermission struct {
	AWSAccountId string
	ActionName   string
}

type AddPermissionResponse struct {
	ResponseMetadata
}

type RemovePermissionResponse struct {
	ResponseMetadata
}

type SetQueueAttributesResponse struct {
	ResponseMetadata
}

type DeleteQueueResponse struct {
	ResponseMetadata
}

// CreateQueue action creates a new queue.
//
// See http://goo.gl/sVUjF for more details
func (s *SQS) CreateQueue(name string, attributes []Attribute) (queue *Queue, err error) {
	resp := &CreateQueueResponse{}
	params := makeParams("CreateQueue")
	queue = nil

	for i, attribute := range attributes {
		params["AttributeName."+strconv.Itoa(i+1)+".Name"] = attribute.Name
		params["AttributeName."+strconv.Itoa(i+1)+".Value"] = attribute.Value
	}

	params["QueueName"] = name
	err = s.query("", params, resp)
	if err != nil {
		return nil, err
	}
	queue = &Queue{s, resp.QueueUrl}
	return queue, err
}

// AddPermission action adds a permission to a queue for a specific principal.
//
// See http://goo.gl/8WBp8 for more details
func (q *Queue) AddPermission(label string, accountPermissions []AccountPermission) (resp *AddPermissionResponse, err error) {
	resp = &AddPermissionResponse{}
	params := makeParams("AddPermission")

	params["Label"] = label
	for i, accountPermission := range accountPermissions {
		params["AWSAccountId."+strconv.Itoa(i+1)] = accountPermission.AWSAccountId
		params["ActionName."+strconv.Itoa(i+1)] = accountPermission.ActionName
	}

	err = q.SQS.query(q.Url, params, resp)
	return
}

// RemovePermission action revokes any permissions in the queue policy that matches the Label parameter.
//
// See http://goo.gl/YLOe8 for more details
func (q *Queue) RemovePermission(label string) (resp *RemovePermissionResponse, err error) {
	resp = &RemovePermissionResponse{}
	params := makeParams("RemovePermission")

	params["Label"] = label
	err = q.SQS.query(q.Url, params, resp)
	return
}

// GetQueueAttributes action returns one or all attributes of a queue.
//
// See http://goo.gl/WejDu for more details
func (q *Queue) GetQueueAttributes(attributes []string) (resp *GetQueueAttributesResponse, err error) {
	resp = &GetQueueAttributesResponse{}
	params := makeParams("GetQueueAttributes")

	for i, attribute := range attributes {
		params["AttributeName."+strconv.Itoa(i+1)] = attribute
	}

	err = q.SQS.query(q.Url, params, resp)
	return
}

// ChangeMessageVisibility action changes the visibility timeout of a specified message in a queue to a new value.
//
// See http://goo.gl/EyJKF for more details
func (q *Queue) ChangeMessageVisibility(receiptHandle string, visibilityTimeout int) (resp *ChangeMessageVisibilityResponse, err error) {
	resp = &ChangeMessageVisibilityResponse{}
	params := makeParams("ChangeMessageVisibility")

	params["VisibilityTimeout"] = strconv.Itoa(visibilityTimeout)
	params["ReceiptHandle"] = receiptHandle

	err = q.SQS.query(q.Url, params, resp)
	return
}

// ChangeMessageVisibilityBatch action is a batch version of the ChangeMessageVisibility action.
//
// See http://goo.gl/pgffn for more details
func (q *Queue) ChangeMessageVisibilityBatch(messageVisibilityBatch []ChangeMessageVisibilityBatchEntry) (resp *ChangeMessageVisibilityBatchResponse, err error) {
	resp = &ChangeMessageVisibilityBatchResponse{}
	params := makeParams("ChangeMessageVisibilityBatch")

	for i, messageVisibility := range messageVisibilityBatch {
		params["ChangeMessageVisibilityBatchRequestEntry."+strconv.Itoa(i+1)+".Id"] = messageVisibility.Id
		params["ChangeMessageVisibilityBatchRequestEntry."+strconv.Itoa(i+1)+".ReceiptHandle"] = messageVisibility.ReceiptHandle
		params["ChangeMessageVisibilityBatchRequestEntry."+strconv.Itoa(i+1)+".VisibilityTimeout"] = strconv.Itoa(messageVisibility.VisibilityTimeout)
	}

	err = q.SQS.query(q.Url, params, resp)
	return
}

// ReceiveMessage action retrieves one or more messages from the specified queue.
//
// See http://goo.gl/ThPrF for more details
func (q *Queue) ReceiveMessage(attributes []string, maxNumberOfMessages int, visibilityTimeout int) (resp *ReceiveMessageResponse, err error) {
	resp = &ReceiveMessageResponse{}
	params := makeParams("ReceiveMessage")

	for i, attribute := range attributes {
		params["AttributeName."+strconv.Itoa(i+1)] = attribute
	}

	params["MaxNumberOfMessages"] = strconv.Itoa(maxNumberOfMessages)
	params["VisibilityTimeout"] = strconv.Itoa(visibilityTimeout)

	err = q.SQS.query(q.Url, params, resp)
	return
}

// DeleteMessage action deletes the specified message from the specified queue.
//
// See http://goo.gl/6XBv7 for more details
func (q *Queue) DeleteMessage(receiptHandle string) (resp *DeleteMessageResponse, err error) {
	resp = &DeleteMessageResponse{}
	params := makeParams("DeleteMessage")

	params["ReceiptHandle"] = receiptHandle

	err = q.SQS.query(q.Url, params, resp)
	return
}

// DeleteMessageBatch action is a batch version of the DeleteMessage action.
//
// See http://goo.gl/y1ehG for more details
func (q *Queue) DeleteMessageBatch(deleteMessageBatch []DeleteMessageBatch) (resp *DeleteMessageBatchResponse, err error) {
	resp = &DeleteMessageBatchResponse{}
	params := makeParams("DeleteMessageBatch")

	for i, deleteMessage := range deleteMessageBatch {
		params["DeleteMessageBatchRequestEntry."+strconv.Itoa(i+1)+".Id"] = deleteMessage.Id
		params["DeleteMessageBatchRequestEntry."+strconv.Itoa(i+1)+".ReceiptHandle"] = deleteMessage.ReceiptHandle
	}

	err = q.SQS.query(q.Url, params, resp)
	return
}

// SendMessage action delivers a message to the specified queue.
// The maximum allowed size is 64KB
//
// See http://goo.gl/7OnPb for more details
func (q *Queue) SendMessage(messageBody string) (resp *SendMessageResponse, err error) {
	resp = &SendMessageResponse{}
	params := makeParams("SendMessage")

	params["MessageBody"] = messageBody
	err = q.SQS.query(q.Url, params, resp)
	return
}

// SendMessageWithDelay is a helper function for SendMessage action which delivers a message to the specified queue
// with a delay.
//
// See http://goo.gl/7OnPb for more details
func (q *Queue) SendMessageWithDelay(messageBody string, delaySeconds int) (resp *SendMessageResponse, err error) {
	resp = &SendMessageResponse{}
	params := makeParams("SendMessage")

	params["MessageBody"] = messageBody
	params["DelaySeconds"] = strconv.Itoa(delaySeconds)
	err = q.SQS.query(q.Url, params, resp)
	return
}

// SendMessageBatch action delivers up to ten messages to the specified queue.
//
// See http://goo.gl/mNytv for more details
func (q *Queue) SendMessageBatch(sendMessageBatchRequests []SendMessageBatchRequestEntry) (resp *SendMessageBatchResponse, err error) {
	resp = &SendMessageBatchResponse{}
	params := makeParams("SendMessageBatch")

	for i, sendMessageBatchRequest := range sendMessageBatchRequests {
		params["SendMessageBatchRequestEntry."+strconv.Itoa(i+1)+".Id"] = sendMessageBatchRequest.Id
		params["SendMessageBatchRequestEntry."+strconv.Itoa(i+1)+".MessageBody"] = sendMessageBatchRequest.MessageBody
		params["SendMessageBatchRequestEntry."+strconv.Itoa(i+1)+".DelaySeconds"] = strconv.Itoa(sendMessageBatchRequest.DelaySeconds)
	}

	err = q.SQS.query(q.Url, params, resp)
	return
}

// Delete action deletes the queue specified by the queue URL, regardless of whether the queue is empty.
//
// See http://goo.gl/c3YCr for more details
func (q *Queue) Delete() (resp *DeleteQueueResponse, err error) {
	resp = &DeleteQueueResponse{}
	params := makeParams("DeleteQueue")

	err = q.SQS.query(q.Url, params, resp)
	return
}

// SetQueueAttributes action sets one attribute of a queue per request.
//
// See http://goo.gl/LyZnj for more details
func (q *Queue) SetQueueAttributes(attribute Attribute) (resp *SetQueueAttributesResponse, err error) {
	resp = &SetQueueAttributesResponse{}
	params := makeParams("SetQueueAttributes")

	params["Attribute.Name"] = attribute.Name
	params["Attribute.Value"] = attribute.Value

	err = q.SQS.query(q.Url, params, resp)
	return
}

// ListQueues  action returns a list of your queues.
//
// See http://goo.gl/RPRWr for more details

func (s *SQS) ListQueues() (resp *ListQueuesResponse, err error) {
	resp = &ListQueuesResponse{}
	params := makeParams("ListQueues")

	err = s.query("", params, resp)
	return
}

// ListQueuesWithPrefix action returns only a list of queues with a name beginning with the specified value are returned
//
// See http://goo.gl/RPRWr for more details
func (s *SQS) ListQueuesWithPrefix(queueNamePrefix string) (resp *ListQueuesResponse, err error) {
	resp = &ListQueuesResponse{}
	params := makeParams("ListQueues")

	if queueNamePrefix != "" {
		params["QueueNamePrefix"] = queueNamePrefix
	}

	err = s.query("", params, resp)
	return
}

// GetQueue is a helper function for GetQueueUrl action that returns an instance of a queue with specified name.
//
// See http://goo.gl/hk7Iu for more details
func (s *SQS) GetQueue(queueName string) (queue *Queue, err error) {
	resp, err := s.GetQueueUrl(queueName)
	if err != nil {
		return nil, err
	}

	queue = &Queue{s, resp.QueueUrl}
	return
}

// GetQueueOfOwner is a helper function for GetQueueUrl action that returns an instance of a queue with specified name
// and belongs to the specified AWS Account Id.
//
// See http://goo.gl/hk7Iu for more details
func (s *SQS) GetQueueOfOwner(queueName, queueOwnerAWSAccountId string) (queue *Queue, err error) {
	resp, err := s.GetQueueUrlOfOwner(queueName, queueOwnerAWSAccountId)
	if err != nil {
		return nil, err
	}

	queue = &Queue{s, resp.QueueUrl}
	return

}

// GetQueueUrl action returns the Uniform Resource Locater (URL) of a queue.
//
// See http://goo.gl/hk7Iu for more details
func (s *SQS) GetQueueUrl(queueName string) (resp *GetQueueUrlResponse, err error) {
	resp = &GetQueueUrlResponse{}
	params := makeParams("GetQueueUrl")

	params["QueueName"] = queueName

	err = s.query("", params, resp)
	return
}

// GetQueueUrlOfOwner is a helper function for GetQueueUrl action that returns the URL of a queue with specified name
// and belongs to the specified AWS Account Id.
//
// See http://goo.gl/hk7Iu for more details for more details
func (s *SQS) GetQueueUrlOfOwner(queueName, queueOwnerAWSAccountId string) (resp *GetQueueUrlResponse, err error) {
	resp = &GetQueueUrlResponse{}
	params := makeParams("GetQueueUrl")

	params["QueueName"] = queueName

	if queueOwnerAWSAccountId != "" {
		params["QueueOwnerAWSAccountId"] = queueOwnerAWSAccountId
	}

	err = s.query("", params, resp)
	return
}

func (s *SQS) query(queueUrl string, params map[string]string, resp interface{}) error {
	params["Version"] = "2011-10-01"
	params["Timestamp"] = time.Now().In(time.UTC).Format(time.RFC3339)
	var endpoint *url.URL
	var path string
	var err error
	if queueUrl != "" {
		endpoint, err = url.Parse(queueUrl)
		path = queueUrl[len(s.Region.SQSEndpoint):]
	} else {
		endpoint, err = url.Parse(s.Region.SQSEndpoint)
		path = "/"
	}
	if err != nil {
		return err
	}

	sign(s.Auth, "GET", path, params, endpoint.Host)
	endpoint.RawQuery = multimap(params).Encode()
	if debug {
		log.Printf("get { %v } -> {\n", endpoint.String())
	}

	r, err := http.Get(endpoint.String())
	if err != nil {
		return err
	}
	defer r.Body.Close()

	if debug {
		dump, _ := httputil.DumpResponse(r, true)
		log.Printf("response:\n")
		log.Printf("%v\n}\n", string(dump))
	}
	if r.StatusCode != 200 {
		return buildError(r)
	}
	err = xml.NewDecoder(r.Body).Decode(resp)
	return err
}

func multimap(p map[string]string) url.Values {
	q := make(url.Values, len(p))
	for k, v := range p {
		q[k] = []string{v}
	}
	return q
}

func buildError(r *http.Response) error {
	errors := xmlErrors{}
	xml.NewDecoder(r.Body).Decode(&errors)
	var err Error
	if len(errors.Errors) > 0 {
		err = errors.Errors[0]
	}
	err.RequestId = errors.RequestId
	err.StatusCode = r.StatusCode
	if err.Message == "" {
		err.Message = r.Status
	}
	return &err
}

func makeParams(action string) map[string]string {
	params := make(map[string]string)
	params["Action"] = action
	return params
}
