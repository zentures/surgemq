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

package topics

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/surgemq/message"
)

func TestNextTopicLevelSuccess(t *testing.T) {
	topics := [][]byte{
		[]byte("sport/tennis/player1/#"),
		[]byte("sport/tennis/player1/ranking"),
		[]byte("sport/#"),
		[]byte("#"),
		[]byte("sport/tennis/#"),
		[]byte("+"),
		[]byte("+/tennis/#"),
		[]byte("sport/+/player1"),
		[]byte("/finance"),
	}

	levels := [][][]byte{
		[][]byte{[]byte("sport"), []byte("tennis"), []byte("player1"), []byte("#")},
		[][]byte{[]byte("sport"), []byte("tennis"), []byte("player1"), []byte("ranking")},
		[][]byte{[]byte("sport"), []byte("#")},
		[][]byte{[]byte("#")},
		[][]byte{[]byte("sport"), []byte("tennis"), []byte("#")},
		[][]byte{[]byte("+")},
		[][]byte{[]byte("+"), []byte("tennis"), []byte("#")},
		[][]byte{[]byte("sport"), []byte("+"), []byte("player1")},
		[][]byte{[]byte("+"), []byte("finance")},
	}

	for i, topic := range topics {
		var (
			tl  []byte
			rem = topic
			err error
		)

		for _, level := range levels[i] {
			tl, rem, err = nextTopicLevel(rem)
			require.NoError(t, err)
			require.Equal(t, level, tl)
		}
	}
}

func TestNextTopicLevelFailure(t *testing.T) {
	topics := [][]byte{
		[]byte("sport/tennis#"),
		[]byte("sport/tennis/#/ranking"),
		[]byte("sport+"),
	}

	var (
		rem []byte
		err error
	)

	_, rem, err = nextTopicLevel(topics[0])
	require.NoError(t, err)

	_, rem, err = nextTopicLevel(rem)
	require.Error(t, err)

	_, rem, err = nextTopicLevel(topics[1])
	require.NoError(t, err)

	_, rem, err = nextTopicLevel(rem)
	require.NoError(t, err)

	_, rem, err = nextTopicLevel(rem)
	require.Error(t, err)

	_, rem, err = nextTopicLevel(topics[2])
	require.Error(t, err)
}

func TestSNodeInsert1(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")

	err := n.sinsert(topic, 1, "sub1")

	require.NoError(t, err)
	require.Equal(t, 1, len(n.snodes))
	require.Equal(t, 0, len(n.subs))

	n2, ok := n.snodes["sport"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.snodes))
	require.Equal(t, 0, len(n2.subs))

	n3, ok := n2.snodes["tennis"]

	require.True(t, ok)
	require.Equal(t, 1, len(n3.snodes))
	require.Equal(t, 0, len(n3.subs))

	n4, ok := n3.snodes["player1"]

	require.True(t, ok)
	require.Equal(t, 1, len(n4.snodes))
	require.Equal(t, 0, len(n4.subs))

	n5, ok := n4.snodes["#"]

	require.True(t, ok)
	require.Equal(t, 0, len(n5.snodes))
	require.Equal(t, 1, len(n5.subs))
	require.Equal(t, "sub1", n5.subs[0].(string))
}

func TestSNodeInsert2(t *testing.T) {
	n := newSNode()
	topic := []byte("#")

	err := n.sinsert(topic, 1, "sub1")

	require.NoError(t, err)
	require.Equal(t, 1, len(n.snodes))
	require.Equal(t, 0, len(n.subs))

	n2, ok := n.snodes["#"]

	require.True(t, ok)
	require.Equal(t, 0, len(n2.snodes))
	require.Equal(t, 1, len(n2.subs))
	require.Equal(t, "sub1", n2.subs[0].(string))
}

func TestSNodeInsert3(t *testing.T) {
	n := newSNode()
	topic := []byte("+/tennis/#")

	err := n.sinsert(topic, 1, "sub1")

	require.NoError(t, err)
	require.Equal(t, 1, len(n.snodes))
	require.Equal(t, 0, len(n.subs))

	n2, ok := n.snodes["+"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.snodes))
	require.Equal(t, 0, len(n2.subs))

	n3, ok := n2.snodes["tennis"]

	require.True(t, ok)
	require.Equal(t, 1, len(n3.snodes))
	require.Equal(t, 0, len(n3.subs))

	n4, ok := n3.snodes["#"]

	require.True(t, ok)
	require.Equal(t, 0, len(n4.snodes))
	require.Equal(t, 1, len(n4.subs))
	require.Equal(t, "sub1", n4.subs[0].(string))
}

func TestSNodeInsert4(t *testing.T) {
	n := newSNode()
	topic := []byte("/finance")

	err := n.sinsert(topic, 1, "sub1")

	require.NoError(t, err)
	require.Equal(t, 1, len(n.snodes))
	require.Equal(t, 0, len(n.subs))

	n2, ok := n.snodes["+"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.snodes))
	require.Equal(t, 0, len(n2.subs))

	n3, ok := n2.snodes["finance"]

	require.True(t, ok)
	require.Equal(t, 0, len(n3.snodes))
	require.Equal(t, 1, len(n3.subs))
	require.Equal(t, "sub1", n3.subs[0].(string))
}

func TestSNodeInsertDup(t *testing.T) {
	n := newSNode()
	topic := []byte("/finance")

	err := n.sinsert(topic, 1, "sub1")
	err = n.sinsert(topic, 1, "sub1")

	require.NoError(t, err)
	require.Equal(t, 1, len(n.snodes))
	require.Equal(t, 0, len(n.subs))

	n2, ok := n.snodes["+"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.snodes))
	require.Equal(t, 0, len(n2.subs))

	n3, ok := n2.snodes["finance"]

	require.True(t, ok)
	require.Equal(t, 0, len(n3.snodes))
	require.Equal(t, 1, len(n3.subs))
	require.Equal(t, "sub1", n3.subs[0].(string))
}

func TestSNodeRemove1(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")

	n.sinsert(topic, 1, "sub1")
	err := n.sremove([]byte("sport/tennis/player1/#"), "sub1")

	require.NoError(t, err)
	require.Equal(t, 0, len(n.snodes))
	require.Equal(t, 0, len(n.subs))
}

func TestSNodeRemove2(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")

	n.sinsert(topic, 1, "sub1")
	err := n.sremove([]byte("sport/tennis/player1"), "sub1")

	require.Error(t, err)
}

func TestSNodeRemove3(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")

	n.sinsert(topic, 1, "sub1")
	n.sinsert(topic, 1, "sub2")
	err := n.sremove([]byte("sport/tennis/player1/#"), nil)

	require.NoError(t, err)
	require.Equal(t, 0, len(n.snodes))
	require.Equal(t, 0, len(n.subs))
}

func TestSNodeMatch1(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")
	n.sinsert(topic, 1, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("sport/tennis/player1/anzel"), 1, &subs, &qoss)

	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
	require.Equal(t, 1, int(qoss[0]))
}

func TestSNodeMatch2(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")
	n.sinsert(topic, 1, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("sport/tennis/player1/anzel"), 1, &subs, &qoss)

	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
	require.Equal(t, 1, int(qoss[0]))
}

func TestSNodeMatch3(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")
	n.sinsert(topic, 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("sport/tennis/player1/anzel"), 2, &subs, &qoss)

	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
	require.Equal(t, 2, int(qoss[0]))
}

func TestSNodeMatch4(t *testing.T) {
	n := newSNode()
	n.sinsert([]byte("sport/tennis/#"), 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("sport/tennis/player1/anzel"), 2, &subs, &qoss)

	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
	require.Equal(t, 2, int(qoss[0]))
}

func TestSNodeMatch5(t *testing.T) {
	n := newSNode()
	n.sinsert([]byte("sport/tennis/+/anzel"), 1, "sub1")
	n.sinsert([]byte("sport/tennis/player1/anzel"), 1, "sub2")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("sport/tennis/player1/anzel"), 1, &subs, &qoss)

	require.NoError(t, err)
	require.Equal(t, 2, len(subs))
}

func TestSNodeMatch6(t *testing.T) {
	n := newSNode()
	n.sinsert([]byte("sport/tennis/#"), 2, "sub1")
	n.sinsert([]byte("sport/tennis"), 1, "sub2")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("sport/tennis/player1/anzel"), 2, &subs, &qoss)

	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
	require.Equal(t, "sub1", subs[0])
}

func TestSNodeMatch7(t *testing.T) {
	n := newSNode()
	n.sinsert([]byte("+/+"), 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("/finance"), 1, &subs, &qoss)

	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
}

func TestSNodeMatch8(t *testing.T) {
	n := newSNode()
	n.sinsert([]byte("/+"), 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("/finance"), 1, &subs, &qoss)

	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
}

func TestSNodeMatch9(t *testing.T) {
	n := newSNode()
	n.sinsert([]byte("+"), 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("/finance"), 1, &subs, &qoss)

	require.NoError(t, err)
	require.Equal(t, 0, len(subs))
}

func TestRNodeInsertRemove(t *testing.T) {
	n := newRNode()

	// --- Insert msg1

	msg := newPublishMessageLarge([]byte("sport/tennis/player1/ricardo"), 1)

	err := n.rinsert(msg.Topic(), msg)

	require.NoError(t, err)
	require.Equal(t, 1, len(n.rnodes))
	require.Nil(t, n.msg)

	n2, ok := n.rnodes["sport"]

	require.True(t, ok)
	require.Equal(t, 1, len(n2.rnodes))
	require.Nil(t, n2.msg)

	n3, ok := n2.rnodes["tennis"]

	require.True(t, ok)
	require.Equal(t, 1, len(n3.rnodes))
	require.Nil(t, n3.msg)

	n4, ok := n3.rnodes["player1"]

	require.True(t, ok)
	require.Equal(t, 1, len(n4.rnodes))
	require.Nil(t, n4.msg)

	n5, ok := n4.rnodes["ricardo"]

	require.True(t, ok)
	require.Equal(t, 0, len(n5.rnodes))
	require.NotNil(t, n5.msg)
	require.Equal(t, msg.QoS(), n5.msg.QoS())
	require.Equal(t, msg.Topic(), n5.msg.Topic())
	require.Equal(t, msg.Payload(), n5.msg.Payload())

	// --- Insert msg2

	msg2 := newPublishMessageLarge([]byte("sport/tennis/player1/andre"), 1)

	err = n.rinsert(msg2.Topic(), msg2)

	require.NoError(t, err)
	require.Equal(t, 2, len(n4.rnodes))

	n6, ok := n4.rnodes["andre"]

	require.True(t, ok)
	require.Equal(t, 0, len(n6.rnodes))
	require.NotNil(t, n6.msg)
	require.Equal(t, msg2.QoS(), n6.msg.QoS())
	require.Equal(t, msg2.Topic(), n6.msg.Topic())

	// --- Remove

	err = n.rremove([]byte("sport/tennis/player1/andre"))
	require.NoError(t, err)
	require.Equal(t, 1, len(n4.rnodes))
}

func TestRNodeMatch(t *testing.T) {
	n := newRNode()

	msg1 := newPublishMessageLarge([]byte("sport/tennis/ricardo/stats"), 1)
	err := n.rinsert(msg1.Topic(), msg1)
	require.NoError(t, err)

	msg2 := newPublishMessageLarge([]byte("sport/tennis/andre/stats"), 1)
	err = n.rinsert(msg2.Topic(), msg2)
	require.NoError(t, err)

	msg3 := newPublishMessageLarge([]byte("sport/tennis/andre/bio"), 1)
	err = n.rinsert(msg3.Topic(), msg3)
	require.NoError(t, err)

	var msglist []*message.PublishMessage

	// ---

	err = n.rmatch(msg1.Topic(), &msglist)

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.rmatch(msg2.Topic(), &msglist)

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.rmatch(msg3.Topic(), &msglist)

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.rmatch([]byte("sport/tennis/andre/+"), &msglist)

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.rmatch([]byte("sport/tennis/andre/#"), &msglist)

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.rmatch([]byte("sport/tennis/+/stats"), &msglist)

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.rmatch([]byte("sport/tennis/#"), &msglist)

	require.NoError(t, err)
	require.Equal(t, 3, len(msglist))
}

func TestMemTopicsSubscription(t *testing.T) {
	Unregister("mem")
	p := NewMemProvider()
	Register("mem", p)

	mgr, err := NewManager("mem")

	MaxQosAllowed = 1
	qos, err := mgr.Subscribe([]byte("sports/tennis/+/stats"), 2, "sub1")

	require.NoError(t, err)
	require.Equal(t, 1, int(qos))

	err = mgr.Unsubscribe([]byte("sports/tennis"), "sub1")

	require.Error(t, err)

	subs := make([]interface{}, 5)
	qoss := make([]byte, 5)

	err = mgr.Subscribers([]byte("sports/tennis/anzel/stats"), 2, &subs, &qoss)

	require.NoError(t, err)
	require.Equal(t, 0, len(subs))

	err = mgr.Subscribers([]byte("sports/tennis/anzel/stats"), 1, &subs, &qoss)

	require.NoError(t, err)
	require.Equal(t, 1, len(subs))
	require.Equal(t, 1, int(qoss[0]))

	err = mgr.Unsubscribe([]byte("sports/tennis/+/stats"), "sub1")

	require.NoError(t, err)
}

func TestMemTopicsRetained(t *testing.T) {
	Unregister("mem")
	p := NewMemProvider()
	Register("mem", p)

	mgr, err := NewManager("mem")
	require.NoError(t, err)
	require.NotNil(t, mgr)

	msg1 := newPublishMessageLarge([]byte("sport/tennis/ricardo/stats"), 1)
	err = mgr.Retain(msg1)
	require.NoError(t, err)

	msg2 := newPublishMessageLarge([]byte("sport/tennis/andre/stats"), 1)
	err = mgr.Retain(msg2)
	require.NoError(t, err)

	msg3 := newPublishMessageLarge([]byte("sport/tennis/andre/bio"), 1)
	err = mgr.Retain(msg3)
	require.NoError(t, err)

	var msglist []*message.PublishMessage

	// ---

	err = mgr.Retained(msg1.Topic(), &msglist)

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained(msg2.Topic(), &msglist)

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained(msg3.Topic(), &msglist)

	require.NoError(t, err)
	require.Equal(t, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/andre/+"), &msglist)

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/andre/#"), &msglist)

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/+/stats"), &msglist)

	require.NoError(t, err)
	require.Equal(t, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/#"), &msglist)

	require.NoError(t, err)
	require.Equal(t, 3, len(msglist))
}

func newPublishMessageLarge(topic []byte, qos byte) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetTopic(topic)
	msg.SetPayload(make([]byte, 1024))
	msg.SetQoS(qos)

	return msg
}
