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

package topics

import (
	"testing"

	"github.com/dataence/assert"
	"github.com/surge/surgemq/message"
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
			rem []byte = topic
			err error
		)

		for _, level := range levels[i] {
			tl, rem, err = nextTopicLevel(rem)
			assert.NoError(t, true, err)
			assert.Equal(t, true, level, tl)
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
	assert.NoError(t, true, err)

	_, rem, err = nextTopicLevel(rem)
	assert.Error(t, true, err)

	_, rem, err = nextTopicLevel(topics[1])
	assert.NoError(t, true, err)

	_, rem, err = nextTopicLevel(rem)
	assert.NoError(t, true, err)

	_, rem, err = nextTopicLevel(rem)
	assert.Error(t, true, err)

	_, rem, err = nextTopicLevel(topics[2])
	assert.Error(t, true, err)
}

func TestSNodeInsert1(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")

	err := n.sinsert(topic, 1, "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(n.snodes))
	assert.Equal(t, true, 0, len(n.subs))

	n2, ok := n.snodes["sport"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 1, len(n2.snodes))
	assert.Equal(t, true, 0, len(n2.subs))

	n3, ok := n2.snodes["tennis"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 1, len(n3.snodes))
	assert.Equal(t, true, 0, len(n3.subs))

	n4, ok := n3.snodes["player1"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 1, len(n4.snodes))
	assert.Equal(t, true, 0, len(n4.subs))

	n5, ok := n4.snodes["#"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 0, len(n5.snodes))
	assert.Equal(t, true, 1, len(n5.subs))
	assert.Equal(t, true, "sub1", n5.subs[0].(string))
}

func TestSNodeInsert2(t *testing.T) {
	n := newSNode()
	topic := []byte("#")

	err := n.sinsert(topic, 1, "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(n.snodes))
	assert.Equal(t, true, 0, len(n.subs))

	n2, ok := n.snodes["#"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 0, len(n2.snodes))
	assert.Equal(t, true, 1, len(n2.subs))
	assert.Equal(t, true, "sub1", n2.subs[0].(string))
}

func TestSNodeInsert3(t *testing.T) {
	n := newSNode()
	topic := []byte("+/tennis/#")

	err := n.sinsert(topic, 1, "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(n.snodes))
	assert.Equal(t, true, 0, len(n.subs))

	n2, ok := n.snodes["+"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 1, len(n2.snodes))
	assert.Equal(t, true, 0, len(n2.subs))

	n3, ok := n2.snodes["tennis"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 1, len(n3.snodes))
	assert.Equal(t, true, 0, len(n3.subs))

	n4, ok := n3.snodes["#"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 0, len(n4.snodes))
	assert.Equal(t, true, 1, len(n4.subs))
	assert.Equal(t, true, "sub1", n4.subs[0].(string))
}

func TestSNodeInsert4(t *testing.T) {
	n := newSNode()
	topic := []byte("/finance")

	err := n.sinsert(topic, 1, "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(n.snodes))
	assert.Equal(t, true, 0, len(n.subs))

	n2, ok := n.snodes["+"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 1, len(n2.snodes))
	assert.Equal(t, true, 0, len(n2.subs))

	n3, ok := n2.snodes["finance"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 0, len(n3.snodes))
	assert.Equal(t, true, 1, len(n3.subs))
	assert.Equal(t, true, "sub1", n3.subs[0].(string))
}

func TestSNodeInsertDup(t *testing.T) {
	n := newSNode()
	topic := []byte("/finance")

	err := n.sinsert(topic, 1, "sub1")
	err = n.sinsert(topic, 1, "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(n.snodes))
	assert.Equal(t, true, 0, len(n.subs))

	n2, ok := n.snodes["+"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 1, len(n2.snodes))
	assert.Equal(t, true, 0, len(n2.subs))

	n3, ok := n2.snodes["finance"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 0, len(n3.snodes))
	assert.Equal(t, true, 1, len(n3.subs))
	assert.Equal(t, true, "sub1", n3.subs[0].(string))
}

func TestSNodeRemove1(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")

	n.sinsert(topic, 1, "sub1")
	err := n.sremove([]byte("sport/tennis/player1/#"), "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 0, len(n.snodes))
	assert.Equal(t, true, 0, len(n.subs))
}

func TestSNodeRemove2(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")

	n.sinsert(topic, 1, "sub1")
	err := n.sremove([]byte("sport/tennis/player1"), "sub1")

	assert.Error(t, true, err)
}

func TestSNodeRemove3(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")

	n.sinsert(topic, 1, "sub1")
	n.sinsert(topic, 1, "sub2")
	err := n.sremove([]byte("sport/tennis/player1/#"), nil)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 0, len(n.snodes))
	assert.Equal(t, true, 0, len(n.subs))
}

func TestSNodeMatch1(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")
	n.sinsert(topic, 1, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("sport/tennis/player1/anzel"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
	assert.Equal(t, true, 1, qoss[0])
}

func TestSNodeMatch2(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")
	n.sinsert(topic, 1, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("sport/tennis/player1/anzel"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
	assert.Equal(t, true, 1, qoss[0])

}

func TestSNodeMatch3(t *testing.T) {
	n := newSNode()
	topic := []byte("sport/tennis/player1/#")
	n.sinsert(topic, 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("sport/tennis/player1/anzel"), 2, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
	assert.Equal(t, true, 2, qoss[0])
}

func TestSNodeMatch4(t *testing.T) {
	n := newSNode()
	n.sinsert([]byte("sport/tennis/#"), 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("sport/tennis/player1/anzel"), 2, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
	assert.Equal(t, true, 2, qoss[0])
}

func TestSNodeMatch5(t *testing.T) {
	n := newSNode()
	n.sinsert([]byte("sport/tennis/+/anzel"), 1, "sub1")
	n.sinsert([]byte("sport/tennis/player1/anzel"), 1, "sub2")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("sport/tennis/player1/anzel"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 2, len(subs))
}

func TestSNodeMatch6(t *testing.T) {
	n := newSNode()
	n.sinsert([]byte("sport/tennis/#"), 2, "sub1")
	n.sinsert([]byte("sport/tennis"), 1, "sub2")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("sport/tennis/player1/anzel"), 2, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
	assert.Equal(t, true, "sub1", subs[0])
}

func TestSNodeMatch7(t *testing.T) {
	n := newSNode()
	n.sinsert([]byte("+/+"), 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("/finance"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
}

func TestSNodeMatch8(t *testing.T) {
	n := newSNode()
	n.sinsert([]byte("/+"), 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("/finance"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
}

func TestSNodeMatch9(t *testing.T) {
	n := newSNode()
	n.sinsert([]byte("+"), 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.smatch([]byte("/finance"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 0, len(subs))
}

func TestRNodeInsertRemove(t *testing.T) {
	n := newRNode()

	// --- Insert msg1

	msg := newPublishMessageLarge([]byte("sport/tennis/player1/ricardo"), 1)

	err := n.rinsert(msg.Topic(), msg)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(n.rnodes))
	assert.Nil(t, true, n.msg)

	n2, ok := n.rnodes["sport"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 1, len(n2.rnodes))
	assert.Nil(t, true, n2.msg)

	n3, ok := n2.rnodes["tennis"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 1, len(n3.rnodes))
	assert.Nil(t, true, n3.msg)

	n4, ok := n3.rnodes["player1"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 1, len(n4.rnodes))
	assert.Nil(t, true, n4.msg)

	n5, ok := n4.rnodes["ricardo"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 0, len(n5.rnodes))
	assert.NotNil(t, true, n5.msg)
	assert.Equal(t, true, msg.QoS(), n5.msg.QoS())
	assert.Equal(t, true, msg.Topic(), n5.msg.Topic())
	assert.Equal(t, true, msg.Payload(), n5.msg.Payload())

	// --- Insert msg2

	msg2 := newPublishMessageLarge([]byte("sport/tennis/player1/andre"), 1)

	err = n.rinsert(msg2.Topic(), msg2)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 2, len(n4.rnodes))

	n6, ok := n4.rnodes["andre"]

	assert.True(t, true, ok)
	assert.Equal(t, true, 0, len(n6.rnodes))
	assert.NotNil(t, true, n6.msg)
	assert.Equal(t, true, msg2.QoS(), n6.msg.QoS())
	assert.Equal(t, true, msg2.Topic(), n6.msg.Topic())

	// --- Remove

	err = n.rremove([]byte("sport/tennis/player1/andre"))
	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(n4.rnodes))
}

func TestRNodeMatch(t *testing.T) {
	n := newRNode()

	msg1 := newPublishMessageLarge([]byte("sport/tennis/ricardo/stats"), 1)
	err := n.rinsert(msg1.Topic(), msg1)
	assert.NoError(t, true, err)

	msg2 := newPublishMessageLarge([]byte("sport/tennis/andre/stats"), 1)
	err = n.rinsert(msg2.Topic(), msg2)
	assert.NoError(t, true, err)

	msg3 := newPublishMessageLarge([]byte("sport/tennis/andre/bio"), 1)
	err = n.rinsert(msg3.Topic(), msg3)
	assert.NoError(t, true, err)

	msglist := make([]*message.PublishMessage, 0)

	// ---

	err = n.rmatch(msg1.Topic(), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.rmatch(msg2.Topic(), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.rmatch(msg3.Topic(), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.rmatch([]byte("sport/tennis/andre/+"), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.rmatch([]byte("sport/tennis/andre/#"), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.rmatch([]byte("sport/tennis/+/stats"), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = n.rmatch([]byte("sport/tennis/#"), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 3, len(msglist))
}

func TestMemTopicsSubscription(t *testing.T) {
	Unregister("mem")
	p := NewMemProvider()
	Register("mem", p)

	mgr, err := NewManager("mem")

	MaxQosAllowed = 1
	qos, err := mgr.Subscribe([]byte("sports/tennis/+/stats"), 2, "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, qos)

	err = mgr.Unsubscribe([]byte("sports/tennis"), "sub1")

	assert.Error(t, true, err)

	subs := make([]interface{}, 5)
	qoss := make([]byte, 5)

	err = mgr.Subscribers([]byte("sports/tennis/anzel/stats"), 2, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 0, len(subs))

	err = mgr.Subscribers([]byte("sports/tennis/anzel/stats"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
	assert.Equal(t, true, 1, qoss[0])

	err = mgr.Unsubscribe([]byte("sports/tennis/+/stats"), "sub1")

	assert.NoError(t, true, err)
}

func TestMemTopicsRetained(t *testing.T) {
	Unregister("mem")
	p := NewMemProvider()
	Register("mem", p)

	mgr, err := NewManager("mem")
	assert.NoError(t, true, err)
	assert.NotNil(t, true, mgr)

	msg1 := newPublishMessageLarge([]byte("sport/tennis/ricardo/stats"), 1)
	err = mgr.Retain(msg1)
	assert.NoError(t, true, err)

	msg2 := newPublishMessageLarge([]byte("sport/tennis/andre/stats"), 1)
	err = mgr.Retain(msg2)
	assert.NoError(t, true, err)

	msg3 := newPublishMessageLarge([]byte("sport/tennis/andre/bio"), 1)
	err = mgr.Retain(msg3)
	assert.NoError(t, true, err)

	msglist := make([]*message.PublishMessage, 0)

	// ---

	err = mgr.Retained(msg1.Topic(), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained(msg2.Topic(), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained(msg3.Topic(), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/andre/+"), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/andre/#"), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/+/stats"), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 2, len(msglist))

	// ---

	msglist = msglist[0:0]
	err = mgr.Retained([]byte("sport/tennis/#"), &msglist)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 3, len(msglist))
}

func newPublishMessageLarge(topic []byte, qos byte) *message.PublishMessage {
	msg := message.NewPublishMessage()
	msg.SetTopic(topic)
	msg.SetPayload(make([]byte, 1024))
	msg.SetQoS(qos)

	return msg
}
