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
	"bytes"
	"io"
	"testing"

	"github.com/dataence/assert"
	"github.com/surge/surgemq/message"
)

func TestReadMessageSuccess(t *testing.T) {
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

	svc := newTestBuffer(t, msgBytes)

	m, n, err := svc.readMessage(message.CONNECT, 62)

	assert.NoError(t, true, err, "Error decoding message.")

	assert.Equal(t, true, len(msgBytes), n, "Error decoding message.")

	msg := m.(*message.ConnectMessage)

	assert.Equal(t, true, 0x1, msg.WillQos(), "Incorrect Will QoS")

	assert.Equal(t, true, 10, msg.KeepAlive(), "Incorrect KeepAlive value.")

	assert.Equal(t, true, "surgemq", string(msg.ClientId()), "Incorrect client ID value.")

	assert.Equal(t, true, "will", string(msg.WillTopic()), "Incorrect will topic value.")

	assert.Equal(t, true, "send me home", string(msg.WillMessage()), "Incorrect will message value.")

	assert.Equal(t, true, "surgemq", string(msg.Username()), "Incorrect username value.")

	assert.Equal(t, true, "verysecret", string(msg.Password()), "Incorrect password value.")
}

// Wrong messag type
func TestReadMessageError(t *testing.T) {
	msgBytes := []byte{
		byte(message.RESERVED << 4), // <--- WRONG
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

	svc := newTestBuffer(t, msgBytes)

	_, _, err := svc.readMessage(message.CONNECT, 62)

	assert.Error(t, true, err)
}

// Wrong messag size
func TestReadMessageError2(t *testing.T) {
	msgBytes := []byte{
		byte(message.CONNECT << 4),
		62, // <--- WRONG
		0,  // Length MSB (0)
		4,  // Length LSB (4)
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

	svc := newTestBuffer(t, msgBytes)

	_, _, err := svc.readMessage(message.CONNECT, 64)

	assert.Equal(t, true, io.EOF, err)
}

func TestWriteMessage(t *testing.T) {
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

	msg := newConnectMessage()
	msg.SetClientId([]byte("surgemq"))
	var err error

	svc := &service{}
	svc.out, err = newBuffer(16384)

	assert.NoError(t, true, err)

	n, err := svc.writeMessage(msg)

	assert.NoError(t, true, err, "error decoding message.")

	assert.Equal(t, true, len(msgBytes), n, "error decoding message.")

	dst, err := svc.out.ReadPeek(len(msgBytes))

	assert.NoError(t, true, err)

	assert.Equal(t, true, msgBytes, dst, "error decoding message.")
}

func newTestBuffer(t *testing.T, msgBytes []byte) *service {
	buf := bytes.NewBuffer(msgBytes)
	svc := &service{}
	var err error

	svc.in, err = newBuffer(16384)

	assert.NoError(t, true, err)

	l, err := svc.in.ReadFrom(buf)

	assert.Equal(t, true, io.EOF, err)
	assert.Equal(t, true, len(msgBytes), l)

	return svc
}
