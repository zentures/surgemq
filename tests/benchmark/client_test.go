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
	"github.com/surge/surgemq/message"
	"github.com/surge/surgemq/service"
)

var (
	messages    int    = 100000
	publishers  int    = 1
	subscribers int    = 1
	size        int    = 1024
	topic       []byte = []byte("test")
	qos         byte   = 0
	nap         int    = 10

	subdone, rcvdone, sentdone int64

	done, done2 chan struct{}

	totalSent,
	totalSentTime,
	totalRcvd,
	totalRcvdTime,
	sentSince,
	rcvdSince int64

	statMu sync.Mutex
)

func init() {
	flag.IntVar(&messages, "messages", messages, "number of messages to send")
	flag.IntVar(&publishers, "publishers", publishers, "number of publishers to start (in FullMesh, only this is used)")
	flag.IntVar(&subscribers, "subscribers", subscribers, "number of subscribers to start (in FullMesh, this is NOT used")
	flag.IntVar(&size, "size", size, "size of message payload to send, minimum 10 bytes")
	flag.Parse()
}

func runClientTest(t testing.TB, cid int, wg *sync.WaitGroup, f func(*service.Client)) {
	defer wg.Done()

	if size < 10 {
		size = 10
	}

	uri := "tcp://127.0.0.1:1883"
	c := connectToServer(t, uri, cid)
	if c == nil {
		return
	}

	if f != nil {
		f(c)
	}

	c.Disconnect()
}

func connectToServer(t testing.TB, uri string, cid int) *service.Client {
	c := &service.Client{}

	msg := newConnectMessage(cid)

	err := c.Connect(uri, msg)
	assert.NoError(t, true, err)

	return c
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
