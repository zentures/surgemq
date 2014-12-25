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

package sessions

import (
	"testing"

	"github.com/dataence/assert"
	"github.com/surge/surgemq/message"
)

func TestSessionInit(t *testing.T) {
	sess := &Session{}
	cmsg := newConnectMessage()

	err := sess.Init(cmsg)
	assert.NoError(t, true, err)
	assert.Equal(t, true, len(sess.cbuf), cmsg.Len())
	assert.Equal(t, true, cmsg.WillQos(), sess.Cmsg.WillQos())
	assert.Equal(t, true, cmsg.Version(), sess.Cmsg.Version())
	assert.Equal(t, true, cmsg.CleanSession(), sess.Cmsg.CleanSession())
	assert.Equal(t, true, cmsg.ClientId(), sess.Cmsg.ClientId())
	assert.Equal(t, true, cmsg.KeepAlive(), sess.Cmsg.KeepAlive())
	assert.Equal(t, true, cmsg.WillTopic(), sess.Cmsg.WillTopic())
	assert.Equal(t, true, cmsg.WillMessage(), sess.Cmsg.WillMessage())
	assert.Equal(t, true, cmsg.Username(), sess.Cmsg.Username())
	assert.Equal(t, true, cmsg.Password(), sess.Cmsg.Password())
	assert.Equal(t, true, []byte("will"), sess.Will.Topic())
	assert.Equal(t, true, cmsg.WillQos(), sess.Will.QoS())

	sess.AddTopic("test", 1)
	assert.Equal(t, true, 1, len(sess.topics))

	topics, qoss, err := sess.Topics()
	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(topics))
	assert.Equal(t, true, 1, len(qoss))
	assert.Equal(t, true, "test", topics[0])
	assert.Equal(t, true, 1, int(qoss[0]))

	sess.RemoveTopic("test")
	assert.Equal(t, true, 0, len(sess.topics))
}

func TestSessionPublishAckqueue(t *testing.T) {
	sess := &Session{}
	cmsg := newConnectMessage()
	err := sess.Init(cmsg)
	assert.NoError(t, true, err)

	for i := 0; i < 12; i++ {
		msg := newPublishMessage(uint16(i), 1)
		sess.Pub1ack.Wait(msg, nil)
	}

	assert.Equal(t, true, 12, sess.Pub1ack.len())

	ack1 := message.NewPubackMessage()
	ack1.SetPacketId(1)
	sess.Pub1ack.Ack(ack1)

	acked := sess.Pub1ack.Acked()
	assert.Equal(t, true, 0, len(acked))

	ack0 := message.NewPubackMessage()
	ack0.SetPacketId(0)
	sess.Pub1ack.Ack(ack0)

	acked = sess.Pub1ack.Acked()
	assert.Equal(t, true, 2, len(acked))
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

func newPublishMessage(pktid uint16, qos byte) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetPacketId(pktid)
	msg.SetTopic([]byte("abc"))
	msg.SetPayload([]byte("abc"))
	msg.SetQoS(qos)

	return msg
}
