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

package message

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublishMessageHeaderFields(t *testing.T) {
	msg := NewPublishMessage()
	msg.mtypeflags[0] |= 11

	require.True(t, msg.Dup(), "Incorrect DUP flag.")
	require.True(t, msg.Retain(), "Incorrect RETAIN flag.")
	require.Equal(t, 1, int(msg.QoS()), "Incorrect QoS.")

	msg.SetDup(false)

	require.False(t, msg.Dup(), "Incorrect DUP flag.")

	msg.SetRetain(false)

	require.False(t, msg.Retain(), "Incorrect RETAIN flag.")

	err := msg.SetQoS(2)

	require.NoError(t, err, "Error setting QoS.")
	require.Equal(t, 2, int(msg.QoS()), "Incorrect QoS.")

	err = msg.SetQoS(3)

	require.Error(t, err)

	err = msg.SetQoS(0)

	require.NoError(t, err, "Error setting QoS.")
	require.Equal(t, 0, int(msg.QoS()), "Incorrect QoS.")

	msg.SetDup(true)

	require.True(t, msg.Dup(), "Incorrect DUP flag.")

	msg.SetRetain(true)

	require.True(t, msg.Retain(), "Incorrect RETAIN flag.")
}

func TestPublishMessageFields(t *testing.T) {
	msg := NewPublishMessage()

	msg.SetTopic([]byte("coolstuff"))

	require.Equal(t, "coolstuff", string(msg.Topic()), "Error setting message topic.")

	err := msg.SetTopic([]byte("coolstuff/#"))

	require.Error(t, err)

	msg.SetPacketId(100)

	require.Equal(t, 100, int(msg.PacketId()), "Error setting acket ID.")

	msg.SetPayload([]byte("this is a payload to be sent"))

	require.Equal(t, []byte("this is a payload to be sent"), msg.Payload(), "Error setting payload.")
}

func TestPublishMessageDecode1(t *testing.T) {
	msgBytes := []byte{
		byte(PUBLISH<<4) | 2,
		23,
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
	}

	msg := NewPublishMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, 7, int(msg.PacketId()), "Error decoding message.")
	require.Equal(t, "surgemq", string(msg.Topic()), "Error deocding topic name.")
	require.Equal(t, []byte{'s', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e'}, msg.Payload(), "Error deocding payload.")
}

// test insufficient bytes
func TestPublishMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(PUBLISH<<4) | 2,
		26,
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
	}

	msg := NewPublishMessage()
	_, err := msg.Decode(msgBytes)

	require.Error(t, err)
}

// test qos = 0 and no client id
func TestPublishMessageDecode3(t *testing.T) {
	msgBytes := []byte{
		byte(PUBLISH << 4),
		21,
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
	}

	msg := NewPublishMessage()
	_, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
}

func TestPublishMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(PUBLISH<<4) | 2,
		23,
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
	}

	msg := NewPublishMessage()
	msg.SetTopic([]byte("surgemq"))
	msg.SetQoS(1)
	msg.SetPacketId(7)
	msg.SetPayload([]byte{'s', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e'})

	dst := make([]byte, 100)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test empty topic name
func TestPublishMessageEncode2(t *testing.T) {
	msg := NewPublishMessage()
	msg.SetTopic([]byte(""))
	msg.SetPacketId(7)
	msg.SetPayload([]byte{'s', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e'})

	dst := make([]byte, 100)
	_, err := msg.Encode(dst)
	require.Error(t, err)
}

// test encoding qos = 0 and no packet id
func TestPublishMessageEncode3(t *testing.T) {
	msgBytes := []byte{
		byte(PUBLISH << 4),
		21,
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
	}

	msg := NewPublishMessage()
	msg.SetTopic([]byte("surgemq"))
	msg.SetQoS(0)
	msg.SetPayload([]byte{'s', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e'})

	dst := make([]byte, 100)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test large message
func TestPublishMessageEncode4(t *testing.T) {
	msgBytes := []byte{
		byte(PUBLISH << 4),
		137,
		8,
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
	}

	payload := make([]byte, 1024)
	msgBytes = append(msgBytes, payload...)

	msg := NewPublishMessage()
	msg.SetTopic([]byte("surgemq"))
	msg.SetQoS(0)
	msg.SetPayload(payload)

	require.Equal(t, len(msgBytes), msg.Len())

	dst := make([]byte, 1100)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test from github issue #2, @mrdg
func TestPublishDecodeEncodeEquiv2(t *testing.T) {
	msgBytes := []byte{50, 18, 0, 9, 103, 114, 101, 101, 116, 105, 110, 103, 115, 0, 1, 72, 101, 108, 108, 111}

	msg := NewPublishMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n2, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestPublishDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(PUBLISH<<4) | 2,
		23,
		0, // topic name MSB (0)
		7, // topic name LSB (7)
		's', 'u', 'r', 'g', 'e', 'm', 'q',
		0, // packet ID MSB (0)
		7, // packet ID LSB (7)
		's', 'e', 'n', 'd', ' ', 'm', 'e', ' ', 'h', 'o', 'm', 'e',
	}

	msg := NewPublishMessage()

	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n2, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n2], "Error decoding message.")

	n3, err := msg.Decode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n3, "Error decoding message.")
}
