// Copyright (c) 2014 Dataence, LLC. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchmark

import (
	"flag"
	"fmt"
	"sync"
	"testing"

	"github.com/dataence/assert"
	"github.com/surge/surgemq/auth"
	"github.com/surge/surgemq/message"
	"github.com/surge/surgemq/service"
	"github.com/surge/surgemq/sessions"
	"github.com/surge/surgemq/topics"
)

var (
	messages int    = 1000000
	clients  int    = 1
	msgsize  int    = 1024
	topic    []byte = []byte("test")
	qos      byte   = 0
	nap      int    = 10
	subdone  int64  = 0

	totalSent,
	totalSentTime,
	totalRcvd,
	totalRcvdTime int64 = 0, 0, 0, 0
)

func init() {
	flag.IntVar(&messages, "messages", messages, "number of messages to send")
	flag.IntVar(&clients, "clients", clients, "number of clients to start")
	flag.IntVar(&msgsize, "msgsize", msgsize, "size of message payload to send")
	flag.Parse()
}

func runClientTest(t testing.TB, cid int, wg *sync.WaitGroup, f func(*service.Client)) {
	defer wg.Done()

	uri := "tcp://127.0.0.1:1883"
	svc := connectToServer(t, uri, cid)

	if f != nil {
		f(svc)
	}

	svc.Disconnect()
}

func connectToServer(t testing.TB, uri string, cid int) *service.Client {
	msg := newConnectMessage(cid)

	svc, err := service.Connect(newTempContext(), uri, msg)
	assert.NoError(t, true, err)

	return svc
}

func newSubscribeMessage(topic string, qos byte) *message.SubscribeMessage {
	msg := message.NewSubscribeMessage()
	msg.SetPacketId(1)
	msg.AddTopic([]byte(topic), qos)

	return msg
}

func newPublishMessageLarge(qos byte) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetTopic([]byte("test"))
	msg.SetPayload(make([]byte, 1024))
	msg.SetQoS(qos)

	return msg
}

func newConnectMessage(cid int) *message.ConnectMessage {
	msg := message.NewConnectMessage()
	msg.SetWillQos(1)
	msg.SetVersion(4)
	msg.SetCleanSession(true)
	msg.SetClientId([]byte(fmt.Sprintf("surgemq%d", cid)))
	msg.SetKeepAlive(10)
	msg.SetWillTopic([]byte("will"))
	msg.SetWillMessage([]byte("send me home"))
	msg.SetUsername([]byte("surgemq"))
	msg.SetPassword([]byte("verysecret"))

	return msg
}

func newTempContext() service.Context {
	return service.Context{
		KeepAlive:      service.DefaultKeepAlive,
		ConnectTimeout: service.DefaultConnectTimeout,
		AckTimeout:     service.DefaultAckTimeout,
		TimeoutRetries: service.DefaultTimeoutRetries,
		Auth:           auth.MockSuccessAuthenticator,
		Topics:         topics.NewMemTopics(),
		Store:          sessions.NewMemStore(),
	}
}
