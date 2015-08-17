// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
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
	"fmt"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/surge/glog"
	"github.com/surgemq/message"
	"github.com/surgemq/surgemq/sessions"
	"github.com/surgemq/surgemq/topics"
)

var (
	gTestClientId uint64 = 0
)

func runClientServerTests(t testing.TB, f func(*Client)) {
	var wg sync.WaitGroup

	ready1 := make(chan struct{})
	ready2 := make(chan struct{})

	uri := "tcp://127.0.0.1:1883"
	u, err := url.Parse(uri)
	require.NoError(t, err, "Error parsing URL")

	// Start listener
	wg.Add(1)
	go startService(t, u, &wg, ready1, ready2)

	<-ready1

	c := connectToServer(t, uri)
	if c == nil {
		return
	}

	defer topics.Unregister(c.svc.sess.ID())

	if f != nil {
		f(c)
	}

	c.Disconnect()

	close(ready2)

	wg.Wait()
}

func startServiceN(t testing.TB, u *url.URL, wg *sync.WaitGroup, ready1, ready2 chan struct{}, cnt int) {
	defer wg.Done()

	topics.Unregister("mem")
	tp := topics.NewMemProvider()
	topics.Register("mem", tp)

	sessions.Unregister("mem")
	sp := sessions.NewMemProvider()
	sessions.Register("mem", sp)

	ln, err := net.Listen(u.Scheme, u.Host)
	require.NoError(t, err)
	defer ln.Close()

	close(ready1)

	svr := &Server{
		Authenticator: authenticator,
	}

	for i := 0; i < cnt; i++ {
		conn, err := ln.Accept()
		require.NoError(t, err)

		_, err = svr.handleConnection(conn)
		if authenticator == "mockFailure" {
			require.Error(t, err)
			return
		} else {
			require.NoError(t, err)
		}
	}

	<-ready2

	for _, svc := range svr.svcs {
		glog.Infof("Stopping service %d", svc.id)
		svc.stop()
	}

}

func startService(t testing.TB, u *url.URL, wg *sync.WaitGroup, ready1, ready2 chan struct{}) {
	startServiceN(t, u, wg, ready1, ready2, 1)
}

func connectToServer(t testing.TB, uri string) *Client {
	c := &Client{}

	msg := newConnectMessage()

	err := c.Connect(uri, msg)
	if authenticator == "mockFailure" {
		require.Error(t, err)
		return nil
	} else {
		require.NoError(t, err)
	}

	return c
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
	msg.AddTopic([]byte("abc"), qos)

	return msg
}

func newUnsubscribeMessage() *message.UnsubscribeMessage {
	msg := message.NewUnsubscribeMessage()
	msg.AddTopic([]byte("abc"))

	return msg
}

func newConnectMessage() *message.ConnectMessage {
	msg := message.NewConnectMessage()
	msg.SetWillQos(1)
	msg.SetVersion(4)
	msg.SetCleanSession(true)
	msg.SetClientId([]byte(fmt.Sprintf("surgemq%d", atomic.AddUint64(&gTestClientId, 1))))
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
