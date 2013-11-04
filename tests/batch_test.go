/**
 * Created with IntelliJ IDEA.
 * User: fun
 * Date: 13-10-22
 * Time: 下午5:49
 * To change this template use File | Settings | File Templates.
 */
package tests

import (
	"fmt"
	"launchpad.net/goamz/aws"
	. "launchpad.net/gocheck"
	"sdk/sqs/sqs"
	"time"
)

/*
func Test(t *testing.T) {
	TestingT(t)
}
*/
var _ = Suite(&BatchSuite{})

type BatchSuite struct {
	sqs *sqs.SQS
}

func (s *BatchSuite) SetUpTest(c *C) {
	fmt.Println("setUp")
	auth := aws.Auth{"abc", "123"}
	s.sqs = sqs.New(auth, aws.Region{SQSEndpoint: "http://sqs.us-east-1.amazonaws.com"})
}

func (s *BatchSuite) TearDownTest(c *C) {
	fmt.Println("fdTear down")
}

func (self *BatchSuite) createQueue(qName string) (queue *sqs.Queue, err error) {
	timeOutAttribute := sqs.Attribute{"VisibilityTimeout", "60"}
	maxMessageSizeAttribute := sqs.Attribute{"MaximumMessageSize", "65536"}
	messageRetentionAttribute := sqs.Attribute{"MessageRetentionPeriod", "345600"}
	delaySeconds := sqs.Attribute{"DelaySeconds", "60"}
	return self.sqs.CreateQueue(qName, []sqs.Attribute{timeOutAttribute, maxMessageSizeAttribute, messageRetentionAttribute, delaySeconds})
}

func (s *BatchSuite) deleteQueue(qName string) error {
	q, err := s.sqs.GetQueue(qName)
	_, err = q.Delete()
	return err
}

func (s *BatchSuite) Test(c *C) {
	fmt.Println("test")
}

func (self *BatchSuite) TestSendMessageBatch(c *C) {
	qName := fmt.Sprintf("TestSendMessageBatch%v", time.Now().UnixNano())
	q, err := self.createQueue(qName)
	defer self.deleteQueue(qName)

	testNum := 10
	var sendMessageBatchRequests []sqs.SendMessageBatchRequestEntry
	for i := 0; i < testNum; i++ {
		sendMessage := sqs.SendMessageBatchRequestEntry{
			Id:           fmt.Sprintf("batchtest%d", i),
			MessageBody:  "hello",
			DelaySeconds: 0,
		}

		sendMessageBatchRequests = append(sendMessageBatchRequests, sendMessage)
	}
	response, err := q.SendMessageBatch(sendMessageBatchRequests)
	c.Assert(err, IsNil)
	fmt.Println(response)

	for {
		res, err := q.ReceiveMessage(nil, 10, 10)
		c.Assert(err, IsNil)
		if len(res.Messages) == 0 {
			break
		}
		c.Assert(len(res.Messages), Equals, 10)
	}

	//test delay second
	sendMessageBatchRequests = make([]sqs.SendMessageBatchRequestEntry, 0)
	for i := 0; i < testNum; i++ {
		sendMessage := sqs.SendMessageBatchRequestEntry{
			Id:           fmt.Sprintf("batchtest%d", i),
			MessageBody:  "hello",
			DelaySeconds: i,
		}

		sendMessageBatchRequests = append(sendMessageBatchRequests, sendMessage)
	}
	response, err = q.SendMessageBatch(sendMessageBatchRequests)
	c.Assert(err, IsNil)
	fmt.Println(response)

	id := 0
	for {
		if id == testNum {
			break
		}
		res, err := q.ReceiveMessage(nil, 1, 10)
		c.Assert(err, IsNil)
		if len(res.Messages) == 0 {
			break
		}
		c.Assert(len(res.Messages), Equals, 1)
		c.Assert(res.Messages[0].Body, Equals, sendMessageBatchRequests[id].MessageBody)
		id++
	}
}
