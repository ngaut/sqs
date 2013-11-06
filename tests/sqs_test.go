package tests

import (
	"fmt"
	"launchpad.net/goamz/aws"
	"launchpad.net/gocheck"
	"sdk/sqs/sqs"
	"strconv"
	"time"
)

var _ = gocheck.Suite(&SqsSimpleTestSuite{})

type SqsSimpleTestSuite struct {
	sqs   *sqs.SQS
}

func (s *SqsSimpleTestSuite) SetUpSuite(c *gocheck.C) {
	auth := aws.Auth{"abc", "123"}
	s.sqs = sqs.New(auth, aws.Region{SQSEndpoint: "http://sqs.us-east-1.amazonaws.com"})
	qName := fmt.Sprintf("testqueue%v", time.Now())
	fmt.Println(qName)
}

func (self *SqsSimpleTestSuite) createQueue(qName string,attributes []sqs.Attribute) (queue *sqs.Queue, err error) {
	return self.sqs.CreateQueue(qName, attributes)
}

func (self *SqsSimpleTestSuite) deleteQueue(qName string) error {
	q, err := self.sqs.GetQueue(qName)
	_, err = q.Delete()
	return err
}

func (self *SqsSimpleTestSuite) TestCreateAndDeleteQueue(c *gocheck.C) {
	qName := fmt.Sprintf("CreateAndDeleteQueue%v", time.Now().UnixNano())
	_, err := self.sqs.CreateQueue(qName,[]sqs.Attribute{})
	c.Assert(err, gocheck.IsNil)

	self.deleteQueue(qName)
}

func (s *SqsSimpleTestSuite) TestListQueues(c *gocheck.C) {
	qName := fmt.Sprintf("TestListQueues%v", time.Now().UnixNano())
	q, err := s.createQueue(qName,[]sqs.Attribute{})

	resp, err := s.sqs.ListQueues()
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(resp.QueueUrl), gocheck.Not(gocheck.Equals), 0)
	//single_url := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s", qName)
	single_url := q.Url
	exist := false
	for _, one_url := range resp.QueueUrl {
		if one_url == single_url {
			exist = true
			break
		}
	}

	c.Assert(exist, gocheck.Equals, true)
	s.deleteQueue(qName)
}

func (s *SqsSimpleTestSuite) TestGetQueueUrl(c *gocheck.C) {
	qName := fmt.Sprintf("TestGetQueueUrl%v", time.Now().UnixNano())
	q, err := s.createQueue(qName,[]sqs.Attribute{})
	single_url := q.Url
	defer s.deleteQueue(qName)

	resp, err := s.sqs.GetQueueUrl(qName)
	c.Assert(err, gocheck.IsNil)
	c.Assert(resp.QueueUrl, gocheck.Equals, single_url)
}

func (s *SqsSimpleTestSuite) TestChangeMessageVisibility(c *gocheck.C) {
	qName := fmt.Sprintf("TestChangeMessageVisibility%v", time.Now().UnixNano())
	q, err := s.createQueue(qName,[]sqs.Attribute{})
	defer s.deleteQueue(qName)

	_, err = q.SendMessage("Hello World")
	c.Assert(err,gocheck.IsNil)

	res,err := q.ReceiveMessage([]string{"All"}, 1, 15)
	c.Assert(err,gocheck.IsNil)

	c.Assert(len(res.Messages)>0, gocheck.Equals, true)

	for _,mess := range res.Messages {
		_, err = q.ChangeMessageVisibility(mess.ReceiptHandle,1)
		c.Assert(err,gocheck.IsNil)
	}

	time.Sleep(2*time.Second)

	for _,mess := range res.Messages {
		_, err = q.DeleteMessage(mess.ReceiptHandle)
		c.Assert(err,gocheck.IsNil)
	}

	res, err = q.ReceiveMessage([]string{"All"},10,15)
	c.Assert(err,gocheck.IsNil)

	for _,mess := range res.Messages {
		_, err = q.DeleteMessage(mess.ReceiptHandle)
		c.Assert(err,gocheck.IsNil)
	}
}


func (self *SqsSimpleTestSuite) TestMessageVisibilityBatch(c *gocheck.C) {
	qName := fmt.Sprintf("testMessageVisibilityBatch%v",time.Now().UnixNano())
	q, err := self.sqs.CreateQueue(qName,[]sqs.Attribute{})
	defer self.deleteQueue(qName)

	testNum := 9
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
	c.Assert(err, gocheck.IsNil)
	fmt.Println(response)


	res, err := q.ReceiveMessage(nil, 10, testNum)
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(res.Messages), gocheck.Equals, testNum)

	var messageVisibilityBatch []sqs.ChangeMessageVisibilityBatchEntry

	for id,message := range res.Messages {
		fmt.Printf("%+v",message)
		curMessage := sqs.ChangeMessageVisibilityBatchEntry {Id:fmt.Sprintf("%d_%s",id,message.MessageId),
										   ReceiptHandle:message.ReceiptHandle,
			VisibilityTimeout:2}
		messageVisibilityBatch = append(messageVisibilityBatch,curMessage)
	}

	cresponse,err := q.ChangeMessageVisibilityBatch(messageVisibilityBatch)
	c.Assert(err,gocheck.IsNil)
	fmt.Println()
	fmt.Println()
	fmt.Printf("%+v\n",cresponse)
	c.Assert(len(cresponse.Id),gocheck.Equals,testNum)


	time.Sleep(3*time.Second)

	//change will failed when message inflight is timeout
	c2response,err := q.ChangeMessageVisibilityBatch(messageVisibilityBatch)
	c.Assert(err,gocheck.IsNil)

	fmt.Printf("%+v\n",c2response)
	c.Assert(len(c2response.Id),gocheck.Equals,0)

}

func (s *SqsSimpleTestSuite) TestSendReceiveDeleteMessage(c *gocheck.C) {
	qName := fmt.Sprintf("TestSendReceiveDeleteMessage%v", time.Now().UnixNano())
	q, err := s.createQueue(qName,[]sqs.Attribute{})
	defer s.deleteQueue(qName)

	resp, err := q.ReceiveMessage([]string{"All"}, 5, 15)
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(resp.Messages), gocheck.Equals, 0)

	total := 100
	step := 5

	for i := 0; i < total; i++ {
		_, err = q.SendMessage("This is a Message")
	}

	for i := 0; i < total/step; i++ {
		resp, err := q.ReceiveMessage([]string{"All"}, step, 15)
		c.Assert(err, gocheck.IsNil)
		c.Assert(len(resp.Messages), gocheck.Equals, step)

		for _, v := range resp.Messages {
			_, err := q.DeleteMessage(v.ReceiptHandle)
			c.Assert(err, gocheck.IsNil)
		}

	}

	resp, err = q.ReceiveMessage([]string{"All"}, step, 15)
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(resp.Messages), gocheck.Equals, 0)

}

func (s *SqsSimpleTestSuite) TestDeleteMessageBatch(c *gocheck.C) {

	qName := fmt.Sprintf("TestDeleteMessageBatch%v", time.Now().UnixNano())
	q, err := s.createQueue(qName,[]sqs.Attribute{})
	defer s.deleteQueue(qName)

	resp, err := q.ReceiveMessage([]string{"All"}, 5, 15)
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(resp.Messages), gocheck.Equals, 0)

	total := 100
	step := 5

	for i := 0; i < total; i++ {
		_, err = q.SendMessage("This is a Message")
	}

	for i := 0; i < total/step; i++ {
		resp, err := q.ReceiveMessage([]string{"All"}, step, 15)
		c.Assert(err, gocheck.IsNil)
		c.Assert(len(resp.Messages), gocheck.Equals, step)

		deleteMessageBatch := make([]sqs.DeleteMessageBatch, 0)

		for _, v := range resp.Messages {
			deleteMessageBatch = append(deleteMessageBatch, sqs.DeleteMessageBatch{Id: strconv.Itoa(i), ReceiptHandle: v.ReceiptHandle})
		}

		{
			_, err := q.DeleteMessageBatch(deleteMessageBatch)
			c.Assert(err, gocheck.IsNil)
		}

	}

	resp, err = q.ReceiveMessage([]string{"All"}, step, 15)
	c.Assert(err, gocheck.IsNil)
	c.Assert(len(resp.Messages), gocheck.Equals, 0)
}

func (s *SqsSimpleTestSuite) TestGetQueueAttributes(c *gocheck.C) {

	qName := fmt.Sprintf("TestGetQueueAttributes%v", time.Now().UnixNano())
	q, err := s.createQueue(qName,[]sqs.Attribute{})
	defer s.deleteQueue(qName)

	resp, err := q.GetQueueAttributes([]string{"ALL"})

	c.Assert(err, gocheck.IsNil)
	c.Assert(len(resp.Attributes), gocheck.Equals, 0)

}

func (s *SqsSimpleTestSuite) TestAddPermission(c *gocheck.C) {
	qName := fmt.Sprintf("TestAddPermission%v", time.Now().UnixNano())
	q, err := s.createQueue(qName,[]sqs.Attribute{})
	defer s.deleteQueue(qName)

	_, err = q.AddPermission("testLabel", []sqs.AccountPermission{sqs.AccountPermission{"125074342641", "SendMessage"}, sqs.AccountPermission{"125074342642", "ReceiveMessage"}})

	c.Assert(err, gocheck.IsNil)

}

func (self *SqsSimpleTestSuite) TestRemovePermission(c *gocheck.C) {
	qName := fmt.Sprintf("TestRemovePermission%v", time.Now().UnixNano())
	q, err := self.createQueue(qName,[]sqs.Attribute{})
	defer self.deleteQueue(qName)
	_, err = q.AddPermission("testLabel", []sqs.AccountPermission{sqs.AccountPermission{"125074342641", "SendMessage"}, sqs.AccountPermission{"125074342642", "ReceiveMessage"}})
	_, err = q.RemovePermission("testLabel")

	c.Assert(err, gocheck.IsNil)

}

func (self *SqsSimpleTestSuite) TestGetQueueAttributesSelective(c *gocheck.C) {
	qName := fmt.Sprintf("TestGetQueueAttributesSelective%v", time.Now().UnixNano())
	timeOut := "60"
	maxMssageSize := "65536"
	messageRetention := "345600"
	delaySe := "60"
	timeOutAttribute := sqs.Attribute{"VisibilityTimeout", timeOut}
	maxMessageSizeAttribute := sqs.Attribute{"MaximumMessageSize", maxMssageSize}
	messageRetentionAttribute := sqs.Attribute{"MessageRetentionPeriod", messageRetention}
	delaySeconds := sqs.Attribute{"DelaySeconds",delaySe}

	q,err := self.sqs.CreateQueue(qName, []sqs.Attribute{timeOutAttribute, maxMessageSizeAttribute, messageRetentionAttribute, delaySeconds})
	defer self.deleteQueue(qName)
	resp, err := q.GetQueueAttributes([]string{"VisibilityTimeout", "DelaySeconds"})

	c.Assert(err, gocheck.IsNil)
	c.Assert(len(resp.Attributes), gocheck.Equals, 2)
	c.Assert(resp.Attributes[0].Value, gocheck.Equals, timeOut)
	c.Assert(resp.Attributes[1].Value, gocheck.Equals, delaySe)

}

func (self *SqsSimpleTestSuite) TestSetQueueAttributes(c *gocheck.C) {

	qName := fmt.Sprintf("TestSetQueueAttributes%v",time.Now().UnixNano())
	q, err := self.sqs.CreateQueue(qName,[]sqs.Attribute{})
	defer self.deleteQueue(qName)

	var policyStr = `
	  {
			"Version":"2008-10-17",
			"Id":"/123456789012/TestSetQueueAttributes/SQSDefaultPolicy",
			"Statement":  [
				 {
				 "Sid":"Queue1ReceiveMessage",
				 "Effect":"Allow",
				 "Principal":{"AWS":"*"},
				 "Action":"SQS:ReceiveMessage",
				 "Resource":"arn:aws:sqs:us-east-1:123456789012:testQueue"
				  }
			 ]
	   }
  `
	_, err = q.SetQueueAttributes(sqs.Attribute{"Policy", policyStr})

	c.Assert(err, gocheck.IsNil)
}
