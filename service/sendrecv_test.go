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
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/surgemq/message"
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

	require.NoError(t, err, "Error decoding message.")

	require.Equal(t, len(msgBytes), n, "Error decoding message.")

	msg := m.(*message.ConnectMessage)

	require.Equal(t, message.QosAtLeastOnce, msg.WillQos(), "Incorrect Will QoS")

	require.Equal(t, 10, int(msg.KeepAlive()), "Incorrect KeepAlive value.")

	require.Equal(t, "surgemq", string(msg.ClientId()), "Incorrect client ID value.")

	require.Equal(t, "will", string(msg.WillTopic()), "Incorrect will topic value.")

	require.Equal(t, "send me home", string(msg.WillMessage()), "Incorrect will message value.")

	require.Equal(t, "surgemq", string(msg.Username()), "Incorrect username value.")

	require.Equal(t, "verysecret", string(msg.Password()), "Incorrect password value.")
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

	require.Error(t, err)
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

	require.Equal(t, io.EOF, err)
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

	require.NoError(t, err)

	n, err := svc.writeMessage(msg)

	require.NoError(t, err, "error decoding message.")

	require.Equal(t, len(msgBytes), n, "error decoding message.")

	dst, err := svc.out.ReadPeek(len(msgBytes))

	require.NoError(t, err)

	require.Equal(t, msgBytes, dst, "error decoding message.")
}

func newTestBuffer(t *testing.T, msgBytes []byte) *service {
	buf := bytes.NewBuffer(msgBytes)
	svc := &service{}
	var err error

	svc.in, err = newBuffer(16384)

	require.NoError(t, err)

	l, err := svc.in.ReadFrom(buf)

	require.Equal(t, io.EOF, err)
	require.Equal(t, int64(len(msgBytes)), l)

	return svc
}
