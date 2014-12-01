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

package message

import (
	"testing"

	"github.com/dataence/assert"
)

func TestConnackMessageFields(t *testing.T) {
	msg := NewConnackMessage()

	msg.SetSessionPresent(true)
	assert.True(t, true, msg.SessionPresent(), "Error setting session present flag.")

	msg.SetSessionPresent(false)
	assert.False(t, true, msg.SessionPresent(), "Error setting session present flag.")

	msg.SetReturnCode(ConnectionAccepted)
	assert.Equal(t, true, ConnectionAccepted, msg.ReturnCode(), "Error setting return code.")
}

func TestConnackMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		2,
		0, // session not present
		0, // connection accepted
	}

	msg := NewConnackMessage()

	n, err := msg.Decode(msgBytes)

	assert.NoError(t, true, err, "Error decoding message.")
	assert.Equal(t, true, len(msgBytes), n, "Error decoding message.")
	assert.False(t, true, msg.SessionPresent(), "Error decoding session present flag.")
	assert.Equal(t, true, ConnectionAccepted, msg.ReturnCode(), "Error decoding return code.")
}

// testing wrong message length
func TestConnackMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		3,
		0, // session not present
		0, // connection accepted
	}

	msg := NewConnackMessage()

	_, err := msg.Decode(msgBytes)
	assert.Error(t, true, err, "Error decoding message.")
}

// testing wrong message size
func TestConnackMessageDecode3(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		2,
		0, // session not present
	}

	msg := NewConnackMessage()

	_, err := msg.Decode(msgBytes)
	assert.Error(t, true, err, "Error decoding message.")
}

// testing wrong reserve bits
func TestConnackMessageDecode4(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		2,
		64, // <- wrong size
		0,  // connection accepted
	}

	msg := NewConnackMessage()

	_, err := msg.Decode(msgBytes)
	assert.Error(t, true, err, "Error decoding message.")
}

// testing invalid return code
func TestConnackMessageDecode5(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		2,
		0,
		6, // <- wrong code
	}

	msg := NewConnackMessage()

	_, err := msg.Decode(msgBytes)
	assert.Error(t, true, err, "Error decoding message.")
}

func TestConnackMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		2,
		1, // session present
		0, // connection accepted
	}

	msg := NewConnackMessage()
	msg.SetReturnCode(ConnectionAccepted)
	msg.SetSessionPresent(true)

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	assert.NoError(t, true, err, "Error decoding message.")
	assert.Equal(t, true, len(msgBytes), n, "Error encoding message.")
	assert.Equal(t, true, msgBytes, dst[:n], "Error encoding connack message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestConnackDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(CONNACK << 4),
		2,
		0, // session not present
		0, // connection accepted
	}

	msg := NewConnackMessage()
	n, err := msg.Decode(msgBytes)

	assert.NoError(t, true, err, "Error decoding message.")
	assert.Equal(t, true, len(msgBytes), n, "Error decoding message.")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	assert.NoError(t, true, err, "Error decoding message.")
	assert.Equal(t, true, len(msgBytes), n2, "Error decoding message.")
	assert.Equal(t, true, msgBytes, dst[:n2], "Error decoding message.")

	n3, err := msg.Decode(dst)

	assert.NoError(t, true, err, "Error decoding message.")
	assert.Equal(t, true, len(msgBytes), n3, "Error decoding message.")
}
