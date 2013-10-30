package tests

import (
	"fmt"
	"launchpad.net/goamz/aws"
	. "launchpad.net/gocheck"
	"sdk/sqs/sqs"
	"strconv"
	"time"
)

var _ = Suite(&S{})

type S struct {
	sqs   *sqs.SQS
	qName string
}

func (s *S) SetUpSuite(c *C) {
	auth := aws.Auth{"abc", "123"}
	s.sqs = sqs.New(auth, aws.Region{SQSEndpoint: "http://sqs.us-east-1.amazonaws.com"})
	qName := fmt.Sprintf("testqueue%v", time.Now())
	fmt.Println(qName)
}

func (self *S) createQueue(qName string) (queue *sqs.Queue, err error) {
	timeOutAttribute := sqs.Attribute{"VisibilityTimeout", "60"}
	maxMessageSizeAttribute := sqs.Attribute{"MaximumMessageSize", "65536"}
	messageRetentionAttribute := sqs.Attribute{"MessageRetentionPeriod", "345600"}
	delaySeconds := sqs.Attribute{"DelaySeconds", "60"}

	return self.sqs.CreateQueue(qName, []sqs.Attribute{timeOutAttribute, maxMessageSizeAttribute, messageRetentionAttribute, delaySeconds})

}

func (self *S) deleteQueue(qName string) error {
	q, err := self.sqs.GetQueue(qName)
	_, err = q.Delete()
	return err
}

func (s *S) TestCreateAndDeleteQueue(c *C) {
	qName := fmt.Sprintf("CreateAndDeleteQueue%v", time.Now().UnixNano())
	q, err := s.createQueue(qName)

	c.Assert(q.Url, Equals, fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s", qName))
	c.Assert(err, IsNil)

	s.deleteQueue(qName)
}

func (s *S) TestListQueues(c *C) {
	qName := fmt.Sprintf("TestListQueues%v", time.Now().UnixNano())
	_, err := s.createQueue(qName)

	resp, err := s.sqs.ListQueues()
	c.Assert(err, IsNil)
	c.Assert(len(resp.QueueUrl), Not(Equals), 0)
	single_url := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s", qName)
	exist := false
	for _, one_url := range resp.QueueUrl {
		if one_url == single_url {
			exist = true
			break
		}
	}

	c.Assert(exist, Equals, true)
	s.deleteQueue(qName)
}

func (s *S) TestGetQueueUrl(c *C) {
	qName := fmt.Sprintf("TestGetQueueUrl%v", time.Now().UnixNano())
	_, err := s.createQueue(qName)
	single_url := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s", qName)
	defer s.deleteQueue(qName)

	resp, err := s.sqs.GetQueueUrl(qName)
	c.Assert(err, IsNil)
	c.Assert(resp.QueueUrl, Equals, single_url)
}

func (s *S) TestChangeMessageVisibility(c *C) {
	qName := fmt.Sprintf("TestChangeMessageVisibility%v", time.Now().UnixNano())
	q, err := s.createQueue(qName)
	//single_url := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s",qName)
	defer s.deleteQueue(qName)

	_, err = q.SendMessage("Hello World")
	c.Assert(err,IsNil)

	res,err := q.ReceiveMessage([]string{"All"}, 1, 15)
	c.Assert(err,IsNil)

	c.Assert(len(res.Messages)>0, Equals, true)

	for _,mess := range res.Messages {
		_, err = q.ChangeMessageVisibility(mess.ReceiptHandle,1)
		c.Assert(err,IsNil)
	}

	time.Sleep(2*time.Second)

	for _,mess := range res.Messages {
		_, err = q.DeleteMessage(mess.ReceiptHandle)
		c.Assert(err,IsNil)
	}

	res, err = q.ReceiveMessage([]string{"All"},10,15)
	c.Assert(err,IsNil)

	for _,mess := range res.Messages {
		_, err = q.DeleteMessage(mess.ReceiptHandle)
		c.Assert(err,IsNil)
	}
}

func (s *S) TestSendReceiveDeleteMessage(c *C) {
	qName := fmt.Sprintf("TestSendReceiveDeleteMessage%v", time.Now().UnixNano())
	q, err := s.createQueue(qName)
	//single_url := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s",qName)
	defer s.deleteQueue(qName)

	resp, err := q.ReceiveMessage([]string{"All"}, 5, 15)
	c.Assert(err, IsNil)
	c.Assert(len(resp.Messages), Equals, 0)

	total := 100
	step := 5

	for i := 0; i < total; i++ {
		_, err = q.SendMessage("This is a Message")
	}

	for i := 0; i < total/step; i++ {
		resp, err := q.ReceiveMessage([]string{"All"}, step, 15)
		c.Assert(err, IsNil)
		c.Assert(len(resp.Messages), Equals, step)

		for _, v := range resp.Messages {
			_, err := q.DeleteMessage(v.ReceiptHandle)
			c.Assert(err, IsNil)
		}

	}

	resp, err = q.ReceiveMessage([]string{"All"}, step, 15)
	c.Assert(err, IsNil)
	c.Assert(len(resp.Messages), Equals, 0)

}

func (s *S) TestDeleteMessageBatch(c *C) {

	qName := fmt.Sprintf("TestDeleteMessageBatch%v", time.Now().UnixNano())
	q, err := s.createQueue(qName)
	//single_url := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s",qName)
	defer s.deleteQueue(qName)

	resp, err := q.ReceiveMessage([]string{"All"}, 5, 15)
	c.Assert(err, IsNil)
	c.Assert(len(resp.Messages), Equals, 0)

	total := 100
	step := 5

	for i := 0; i < total; i++ {
		_, err = q.SendMessage("This is a Message")
	}

	for i := 0; i < total/step; i++ {
		resp, err := q.ReceiveMessage([]string{"All"}, step, 15)
		c.Assert(err, IsNil)
		c.Assert(len(resp.Messages), Equals, step)

		deleteMessageBatch := make([]sqs.DeleteMessageBatch, 0)

		for _, v := range resp.Messages {
			deleteMessageBatch = append(deleteMessageBatch, sqs.DeleteMessageBatch{Id: strconv.Itoa(i), ReceiptHandle: v.ReceiptHandle})
		}

		{
			_, err := q.DeleteMessageBatch(deleteMessageBatch)
			c.Assert(err, IsNil)
		}

	}

	resp, err = q.ReceiveMessage([]string{"All"}, step, 15)
	c.Assert(err, IsNil)
	c.Assert(len(resp.Messages), Equals, 0)
}

/*
func (s *S) TestChangeMessageVisibilityBatch(c *C) {
	testServer.PrepareResponse(200, nil, TestCreateQueueXmlOK)

	timeOutAttribute := sqs.Attribute{"VisibilityTimeout", "60"}
	maxMessageSizeAttribute := sqs.Attribute{"MaximumMessageSize", "65536"}
	messageRetentionAttribute := sqs.Attribute{"MessageRetentionPeriod", "345600"}
	q, err := s.sqs.CreateQueue("testQueue", []sqs.Attribute{timeOutAttribute, maxMessageSizeAttribute, messageRetentionAttribute})
	testServer.WaitRequest()

	testServer.PrepareResponse(200, nil, TestChangeMessaveVisibilityBatchXmlOK)

	messageVisibilityBatch := []sqs.ChangeMessageVisibilityBatchEntry{sqs.ChangeMessageVisibilityBatchEntry{"change_visibility_msg_2", "gfk0T0R0waama4fVFffkjKzmhMCymjQvfTFk2LxT33G4ms5subrE0deLKWSscPU1oD3J9zgeS4PQQ3U30qOumIE6AdAv3w%2F%2Fa1IXW6AqaWhGsEPaLm3Vf6IiWqdM8u5imB%2BNTwj3tQRzOWdTOePjOjPcTpRxBtXix%2BEvwJOZUma9wabv%2BSw6ZHjwmNcVDx8dZXJhVp16Bksiox%2FGrUvrVTCJRTWTLc59oHLLF8sEkKzRmGNzTDGTiV%2BYjHfQj60FD3rVaXmzTsoNxRhKJ72uIHVMGVQiAGgBX6HGv9LDmYhPXw4hy%2FNgIg%3D%3D", 45}, sqs.ChangeMessageVisibilityBatchEntry{"change_visibility_msg_3", "gfk0T0R0waama4fVFffkjKzmhMCymjQvfTFk2LxT33FUgBz3%2BnougdeLKWSscPU1%2FXgx%2BxcNnjnQQ3U30qOumIE6AdAv3w%2F%2Fa1IXW6AqaWhGsEPaLm3Vf6IiWqdM8u5imB%2BNTwj3tQRzOWdTOePjOsogjZM%2F7kzn4Ew27XLU9I%2FYaWYmKvDbq%2Fk3HKVB9HfB43kE49atP2aWrzNL4yunG41Q4cfRRtfJdcGQGNHQ2%2Byd0Usf5qR1dZr1iDo5xk946eQat83AxTRP%2BY4Qi0V7FAeSLH9su9xpX6HGv9LDmYhPXw4hy%2FNgIg%3D%3D", 45}}
	resp, err := q.ChangeMessageVisibilityBatch(messageVisibilityBatch)
	testServer.WaitRequest()
	c.Assert(err, IsNil)
	c.Assert(resp.ResponseMetadata.RequestId, Equals, "ca9668f7-ab1b-4f7a-8859-f15747ab17a7")
	c.Assert(resp.Id[0], Equals, "change_visibility_msg_2")
	c.Assert(resp.Id[1], Equals, "change_visibility_msg_3")
}



func (s *S) TestSendMessageWithDelay(c *C) {
	testServer.PrepareResponse(200, nil, TestGetQueueUrlXmlOK)

	q, err := s.sqs.GetQueue("testQueue")
	testServer.WaitRequest()

	testServer.PrepareResponse(200, nil, TestSendMessageXmlOK)

	resp, err := q.SendMessageWithDelay("This is a Message", 60)
	testServer.WaitRequest()
	c.Assert(err, IsNil)
	c.Assert(resp.SendMessageResult.MD5OfMessageBody, Equals, "fafb00f5732ab283681e124bf8747ed1")
	c.Assert(resp.SendMessageResult.MessageId, Equals, "5fea7756-0ea4-451a-a703-a558b933e274")
	c.Assert(resp.ResponseMetadata.RequestId, Equals, "27daac76-34dd-47df-bd01-1f6e873584a0")
}
*/

func (s *S) TestGetQueueAttributes(c *C) {

	qName := fmt.Sprintf("TestGetQueueAttributes%v", time.Now().UnixNano())
	q, err := s.createQueue(qName)
	//single_url := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s",qName)
	defer s.deleteQueue(qName)

	resp, err := q.GetQueueAttributes([]string{"ALL"})

	c.Assert(err, IsNil)
	c.Assert(len(resp.Attributes), Equals, 4)

}

func (s *S) TestAddPermission(c *C) {
	qName := fmt.Sprintf("TestAddPermission%v", time.Now().UnixNano())
	q, err := s.createQueue(qName)
	//single_url := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s",qName)
	defer s.deleteQueue(qName)

	_, err = q.AddPermission("testLabel", []sqs.AccountPermission{sqs.AccountPermission{"125074342641", "SendMessage"}, sqs.AccountPermission{"125074342642", "ReceiveMessage"}})

	c.Assert(err, IsNil)

}

func (s *S) TestRemovePermission(c *C) {
	qName := fmt.Sprintf("TestRemovePermission%v", time.Now().UnixNano())
	q, err := s.createQueue(qName)
	//single_url := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s",qName)
	defer s.deleteQueue(qName)
	_, err = q.AddPermission("testLabel", []sqs.AccountPermission{sqs.AccountPermission{"125074342641", "SendMessage"}, sqs.AccountPermission{"125074342642", "ReceiveMessage"}})
	_, err = q.RemovePermission("testLabel")

	c.Assert(err, IsNil)

}

func (s *S) TestGetQueueAttributesSelective(c *C) {
	qName := fmt.Sprintf("TestGetQueueAttributesSelective%v", time.Now().UnixNano())
	q, err := s.createQueue(qName)
	//single_url := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s",qName)
	defer s.deleteQueue(qName)
	resp, err := q.GetQueueAttributes([]string{"VisibilityTimeout", "DelaySeconds"})

	c.Assert(err, IsNil)
	c.Assert(len(resp.Attributes), Equals, 2)
	// c.Assert(resp.Attributes[0].Name, Equals, "VisibilityTimeout")
	c.Assert(resp.Attributes[0].Value, Equals, "60")
	// c.Assert(resp.Attributes[1].Name, Equals, "DelaySeconds")
	c.Assert(resp.Attributes[1].Value, Equals, "60")

}

func (s *S) TestSetQueueAttributes(c *C) {

	qName := "TestSetQueueAttributes"
	q, err := s.createQueue(qName)
	//single_url := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s",qName)
	defer s.deleteQueue(qName)

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

	c.Assert(err, IsNil)
}
