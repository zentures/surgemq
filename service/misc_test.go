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

package service

import (
	"bufio"
	"bytes"
	"net"
	"net/url"
	"sync"
	"testing"

	"github.com/dataence/assert"
	"github.com/surge/surgemq/auth"
	"github.com/surge/surgemq/message"
	"github.com/surge/surgemq/sessions"
	"github.com/surge/surgemq/topics"
)

func runClientServerTests(t testing.TB, f func(*Client)) {
	var wg sync.WaitGroup

	ready1 := make(chan struct{})
	ready2 := make(chan struct{})

	uri := "tcp://127.0.0.1:1883"
	u, err := url.Parse(uri)
	assert.NoError(t, true, err, "Error parsing URL")

	// Start listener
	wg.Add(1)
	go startService(t, u, newTempContext(), &wg, ready1, ready2)

	<-ready1

	svc := connectToServer(t, uri, true)

	if f != nil {
		f(svc)
	}

	svc.Disconnect()

	close(ready2)

	wg.Wait()
}

func startService(t testing.TB, u *url.URL, ctx Context, wg *sync.WaitGroup, ready1, ready2 chan struct{}) {
	defer wg.Done()

	conn := listenAndConnect(t, u, ready1)

	svc, err := handleConnection(ctx, conn)
	defer svc.Disconnect()
	assert.NoError(t, true, err)

	<-ready2
}

func listenAndConnect(t testing.TB, u *url.URL, ready chan struct{}) net.Conn {
	ln, err := net.Listen(u.Scheme, u.Host)
	assert.NoError(t, true, err)
	defer ln.Close()

	close(ready)

	conn, err := ln.Accept()
	assert.NoError(t, true, err)

	return conn
}

func connectToServer(t testing.TB, uri string, success bool) *Client {
	msg := newConnectMessage()

	svc, err := Connect(newTempContext(), uri, msg)
	if success {
		assert.NoError(t, true, err)
	} else {
		assert.Error(t, true, err)
		return nil
	}

	return svc
}

func newPubrelMessage(pktid uint16) *message.PubrelMessage {
	msg := message.NewPubrelMessage()
	msg.SetPacketId(pktid)

	return msg
}

func newPublishMessage(pktid uint16, qos byte) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetPacketId(pktid)
	msg.SetTopic([]byte("abc"))
	msg.SetPayload([]byte("abc"))
	msg.SetQoS(qos)

	return msg
}

func newPublishMessageLarge(pktid uint16, qos byte) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetPacketId(pktid)
	msg.SetTopic([]byte("abc"))
	msg.SetPayload(make([]byte, 1024))
	msg.SetQoS(qos)

	return msg
}

func newSubscribeMessage(qos byte) *message.SubscribeMessage {
	msg := message.NewSubscribeMessage()
	msg.SetPacketId(1)
	msg.AddTopic([]byte("abc"), qos)

	return msg
}

func newUnsubscribeMessage() *message.UnsubscribeMessage {
	msg := message.NewUnsubscribeMessage()
	msg.SetPacketId(1)
	msg.AddTopic([]byte("abc"))

	return msg
}

func newConnectMessage() *message.ConnectMessage {
	msg := message.NewConnectMessage()
	msg.SetWillQos(1)
	msg.SetVersion(4)
	msg.SetCleanSession(true)
	msg.SetClientId([]byte("surgemq"))
	msg.SetKeepAlive(10)
	msg.SetWillTopic([]byte("will"))
	msg.SetWillMessage([]byte("send me home"))
	msg.SetUsername([]byte("surgemq"))
	msg.SetPassword([]byte("verysecret"))

	return msg
}

func newConnectMessageBuffer() *bufio.Reader {
	msgBytes := []byte{
		byte(message.CONNECT << 4),
		60,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		206, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		7,   // Client ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // Will Topic MSB (0)
		4, // Will Topic LSB (4)
		'w', 'i', 'l', 'l',
		0,  // Will Message MSB (0)
		12, // Will Message LSB (12)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
		0, // Username ID MSB (0)
		7, // Username ID LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0,  // Password ID MSB (0)
		10, // Password ID LSB (10)
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e', 't',
	}

	return bufio.NewReader(bytes.NewBuffer(msgBytes))
}

func newTempContext() Context {
	return Context{
		KeepAlive:      DefaultKeepAlive,
		ConnectTimeout: DefaultConnectTimeout,
		AckTimeout:     DefaultAckTimeout,
		TimeoutRetries: DefaultTimeoutRetries,
		Auth:           auth.MockSuccessAuthenticator,
		Topics:         topics.NewMemTopics(),
		Store:          sessions.NewMemStore(),
	}
}
