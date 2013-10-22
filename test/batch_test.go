/**
 * Created with IntelliJ IDEA.
 * User: fun
 * Date: 13-10-22
 * Time: 下午5:49
 * To change this template use File | Settings | File Templates.
 */
package test

import (
	"sdk/sqs/sqs"
	"launchpad.net/goamz/aws"
	. "launchpad.net/gocheck"
	"fmt"
	"testing"
)

func Test(t *testing.T) {
	TestingT(t)
}

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

func (self *BatchSuite) createQueue() (queue *sqs.Queue, err error) {
	timeOutAttribute := sqs.Attribute{"VisibilityTimeout", "60"}
    maxMessageSizeAttribute := sqs.Attribute{"MaximumMessageSize", "65536"}
    messageRetentionAttribute := sqs.Attribute{"MessageRetentionPeriod", "345600"}
    delaySeconds := sqs.Attribute{"DelaySeconds", "60"}
    return self.sqs.CreateQueue("testQueue", []sqs.Attribute{timeOutAttribute, maxMessageSizeAttribute, messageRetentionAttribute, delaySeconds})
}

func (s *BatchSuite) deleteQueue(qName string ) error {
	q, err := s.sqs.GetQueue(qName)
	_, err = q.Delete()
	return err
}

func (s *BatchSuite) Test(c *C){
	fmt.Println("test")
}

func (self *BatchSuite) TestSendMessageBatch(c *C) {
	testNum := 10
	q, err := self.createQueue()
	var sendMessageBatchRequests []sqs.SendMessageBatchRequestEntry
	for i := 0; i < testNum; i ++ {
		sendMessage := sqs.SendMessageBatchRequestEntry{
	Id:fmt.Sprintf("batchtest%d",i),
	MessageBody:"hello",
		DelaySeconds:10,
	}

	sendMessageBatchRequests = append(sendMessageBatchRequests,sendMessage)
    }

	response,err := q.SendMessageBatch(sendMessageBatchRequests)
	c.Assert(err,IsNil)
	fmt.Println(response)


}


