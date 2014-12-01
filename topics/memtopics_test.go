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

func TestNodeInsert1(t *testing.T) {
	n := newNode()
	topic := []byte("sport/tennis/player1/#")

	err := n.insert(topic, 1, "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(n.nodes))
	assert.Equal(t, true, 0, len(n.subs))

	n2, ok := n.nodes["sport"]

	assert.Equal(t, true, true, ok)
	assert.Equal(t, true, 1, len(n2.nodes))
	assert.Equal(t, true, 0, len(n2.subs))

	n3, ok := n2.nodes["tennis"]

	assert.Equal(t, true, true, ok)
	assert.Equal(t, true, 1, len(n3.nodes))
	assert.Equal(t, true, 0, len(n3.subs))

	n4, ok := n3.nodes["player1"]

	assert.Equal(t, true, true, ok)
	assert.Equal(t, true, 1, len(n4.nodes))
	assert.Equal(t, true, 0, len(n4.subs))

	n5, ok := n4.nodes["#"]

	assert.Equal(t, true, true, ok)
	assert.Equal(t, true, 0, len(n5.nodes))
	assert.Equal(t, true, 1, len(n5.subs))
	assert.Equal(t, true, "sub1", n5.subs[0].(string))
}

func TestNodeInsert2(t *testing.T) {
	n := newNode()
	topic := []byte("#")

	err := n.insert(topic, 1, "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(n.nodes))
	assert.Equal(t, true, 0, len(n.subs))

	n2, ok := n.nodes["#"]

	assert.Equal(t, true, true, ok)
	assert.Equal(t, true, 0, len(n2.nodes))
	assert.Equal(t, true, 1, len(n2.subs))
	assert.Equal(t, true, "sub1", n2.subs[0].(string))
}

func TestNodeInsert3(t *testing.T) {
	n := newNode()
	topic := []byte("+/tennis/#")

	err := n.insert(topic, 1, "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(n.nodes))
	assert.Equal(t, true, 0, len(n.subs))

	n2, ok := n.nodes["+"]

	assert.Equal(t, true, true, ok)
	assert.Equal(t, true, 1, len(n2.nodes))
	assert.Equal(t, true, 0, len(n2.subs))

	n3, ok := n2.nodes["tennis"]

	assert.Equal(t, true, true, ok)
	assert.Equal(t, true, 1, len(n3.nodes))
	assert.Equal(t, true, 0, len(n3.subs))

	n4, ok := n3.nodes["#"]

	assert.Equal(t, true, true, ok)
	assert.Equal(t, true, 0, len(n4.nodes))
	assert.Equal(t, true, 1, len(n4.subs))
	assert.Equal(t, true, "sub1", n4.subs[0].(string))
}

func TestNodeInsert4(t *testing.T) {
	n := newNode()
	topic := []byte("/finance")

	err := n.insert(topic, 1, "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(n.nodes))
	assert.Equal(t, true, 0, len(n.subs))

	n2, ok := n.nodes["+"]

	assert.Equal(t, true, true, ok)
	assert.Equal(t, true, 1, len(n2.nodes))
	assert.Equal(t, true, 0, len(n2.subs))

	n3, ok := n2.nodes["finance"]

	assert.Equal(t, true, true, ok)
	assert.Equal(t, true, 0, len(n3.nodes))
	assert.Equal(t, true, 1, len(n3.subs))
	assert.Equal(t, true, "sub1", n3.subs[0].(string))
}

func TestNodeInsertDup(t *testing.T) {
	n := newNode()
	topic := []byte("/finance")

	err := n.insert(topic, 1, "sub1")
	err = n.insert(topic, 1, "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(n.nodes))
	assert.Equal(t, true, 0, len(n.subs))

	n2, ok := n.nodes["+"]

	assert.Equal(t, true, true, ok)
	assert.Equal(t, true, 1, len(n2.nodes))
	assert.Equal(t, true, 0, len(n2.subs))

	n3, ok := n2.nodes["finance"]

	assert.Equal(t, true, true, ok)
	assert.Equal(t, true, 0, len(n3.nodes))
	assert.Equal(t, true, 1, len(n3.subs))
	assert.Equal(t, true, "sub1", n3.subs[0].(string))
}

func TestNodeRemove1(t *testing.T) {
	n := newNode()
	topic := []byte("sport/tennis/player1/#")

	n.insert(topic, 1, "sub1")
	err := n.remove([]byte("sport/tennis/player1/#"), "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 0, len(n.nodes))
	assert.Equal(t, true, 0, len(n.subs))
}

func TestNodeRemove2(t *testing.T) {
	n := newNode()
	topic := []byte("sport/tennis/player1/#")

	n.insert(topic, 1, "sub1")
	err := n.remove([]byte("sport/tennis/player1"), "sub1")

	assert.Error(t, true, err)
}

func TestNodeRemove3(t *testing.T) {
	n := newNode()
	topic := []byte("sport/tennis/player1/#")

	n.insert(topic, 1, "sub1")
	n.insert(topic, 1, "sub2")
	err := n.remove([]byte("sport/tennis/player1/#"), nil)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 0, len(n.nodes))
	assert.Equal(t, true, 0, len(n.subs))
}

func TestNodeMatch1(t *testing.T) {
	n := newNode()
	topic := []byte("sport/tennis/player1/#")
	n.insert(topic, 1, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.match([]byte("sport/tennis/player1/anzel"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
	assert.Equal(t, true, 1, qoss[0])
}

func TestNodeMatch2(t *testing.T) {
	n := newNode()
	topic := []byte("sport/tennis/player1/#")
	n.insert(topic, 1, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.match([]byte("sport/tennis/player1/anzel"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
	assert.Equal(t, true, 1, qoss[0])

}

func TestNodeMatch3(t *testing.T) {
	n := newNode()
	topic := []byte("sport/tennis/player1/#")
	n.insert(topic, 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.match([]byte("sport/tennis/player1/anzel"), 2, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
	assert.Equal(t, true, 2, qoss[0])
}

func TestNodeMatch4(t *testing.T) {
	n := newNode()
	n.insert([]byte("sport/tennis/#"), 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.match([]byte("sport/tennis/player1/anzel"), 2, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
	assert.Equal(t, true, 2, qoss[0])
}

func TestNodeMatch5(t *testing.T) {
	n := newNode()
	n.insert([]byte("sport/tennis/+/anzel"), 1, "sub1")
	n.insert([]byte("sport/tennis/player1/anzel"), 1, "sub2")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.match([]byte("sport/tennis/player1/anzel"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 2, len(subs))
}

func TestNodeMatch6(t *testing.T) {
	n := newNode()
	n.insert([]byte("sport/tennis/#"), 2, "sub1")
	n.insert([]byte("sport/tennis"), 1, "sub2")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.match([]byte("sport/tennis/player1/anzel"), 2, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
	assert.Equal(t, true, "sub1", subs[0])
}

func TestNodeMatch7(t *testing.T) {
	n := newNode()
	n.insert([]byte("+/+"), 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.match([]byte("/finance"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
}

func TestNodeMatch8(t *testing.T) {
	n := newNode()
	n.insert([]byte("/+"), 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.match([]byte("/finance"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
}

func TestNodeMatch9(t *testing.T) {
	n := newNode()
	n.insert([]byte("+"), 2, "sub1")

	subs := make([]interface{}, 0, 5)
	qoss := make([]byte, 0, 5)

	err := n.match([]byte("/finance"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 0, len(subs))
}

func TestMemTopics(t *testing.T) {
	topics := NewMemTopics()
	MaxQosAllowed = 1
	qos, err := topics.Subscribe([]byte("sports/tennis/+/stats"), 2, "sub1")

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, qos)

	err = topics.Unsubscribe([]byte("sports/tennis"), "sub1")

	assert.Error(t, true, err)

	subs := make([]interface{}, 5)
	qoss := make([]byte, 5)

	err = topics.Subscribers([]byte("sports/tennis/anzel/stats"), 2, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 0, len(subs))

	err = topics.Subscribers([]byte("sports/tennis/anzel/stats"), 1, &subs, &qoss)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 1, len(subs))
	assert.Equal(t, true, 1, qoss[0])

	err = topics.Unsubscribe([]byte("sports/tennis/+/stats"), "sub1")

	assert.NoError(t, true, err)
}
