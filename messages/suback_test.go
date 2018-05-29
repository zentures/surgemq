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

func TestSubackMessageFields(t *testing.T) {
	msg := NewSubackMessage()

	msg.SetPacketId(100)
	require.Equal(t, 100, int(msg.PacketId()), "Error setting packet ID.")

	msg.AddReturnCode(1)
	require.Equal(t, 1, len(msg.ReturnCodes()), "Error adding return code.")

	err := msg.AddReturnCode(0x90)
	require.Error(t, err)
}

func TestSubackMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(SUBACK << 4),
		6,
		0,    // packet ID MSB (0)
		7,    // packet ID LSB (7)
		0,    // return code 1
		1,    // return code 2
		2,    // return code 3
		0x80, // return code 4
	}

	msg := NewSubackMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, SUBACK, msg.Type(), "Error decoding message.")
	require.Equal(t, 4, len(msg.ReturnCodes()), "Error adding return code.")
}

// test with wrong return code
func TestSubackMessageDecode2(t *testing.T) {
	msgBytes := []byte{
		byte(SUBACK << 4),
		6,
		0,    // packet ID MSB (0)
		7,    // packet ID LSB (7)
		0,    // return code 1
		1,    // return code 2
		2,    // return code 3
		0x81, // return code 4
	}

	msg := NewSubackMessage()
	_, err := msg.Decode(msgBytes)

	require.Error(t, err)
}

func TestSubackMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(SUBACK << 4),
		6,
		0,    // packet ID MSB (0)
		7,    // packet ID LSB (7)
		0,    // return code 1
		1,    // return code 2
		2,    // return code 3
		0x80, // return code 4
	}

	msg := NewSubackMessage()
	msg.SetPacketId(7)
	msg.AddReturnCode(0)
	msg.AddReturnCode(1)
	msg.AddReturnCode(2)
	msg.AddReturnCode(0x80)

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding message.")
	require.Equal(t, len(msgBytes), n, "Error decoding message.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestSubackDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(SUBACK << 4),
		6,
		0,    // packet ID MSB (0)
		7,    // packet ID LSB (7)
		0,    // return code 1
		1,    // return code 2
		2,    // return code 3
		0x80, // return code 4
	}

	msg := NewSubackMessage()
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
