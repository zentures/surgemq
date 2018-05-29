// Copyright (c) 2018 Zentures, LLC. All rights reserved.
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

package messages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPingreqMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(PINGREQ << 4),
		0,
	}

	msg := NewPingreqMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding messages.")
	require.Equal(t, len(msgBytes), n, "Error decoding messages.")
	require.Equal(t, PINGREQ, msg.Type(), "Error decoding messages.")
}

func TestPingreqMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(PINGREQ << 4),
		0,
	}

	msg := NewPingreqMessage()

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding messages.")
	require.Equal(t, len(msgBytes), n, "Error decoding messages.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding messages.")
}

func TestPingrespMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(PINGRESP << 4),
		0,
	}

	msg := NewPingrespMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding messages.")
	require.Equal(t, len(msgBytes), n, "Error decoding messages.")
	require.Equal(t, PINGRESP, msg.Type(), "Error decoding messages.")
}

func TestPingrespMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(PINGRESP << 4),
		0,
	}

	msg := NewPingrespMessage()

	dst := make([]byte, 10)
	n, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding messages.")
	require.Equal(t, len(msgBytes), n, "Error decoding messages.")
	require.Equal(t, msgBytes, dst[:n], "Error decoding messages.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestPingreqDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(PINGREQ << 4),
		0,
	}

	msg := NewPingreqMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding messages.")
	require.Equal(t, len(msgBytes), n, "Error decoding messages.")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding messages.")
	require.Equal(t, len(msgBytes), n2, "Error decoding messages.")
	require.Equal(t, msgBytes, dst[:n2], "Error decoding messages.")

	n3, err := msg.Decode(dst)

	require.NoError(t, err, "Error decoding messages.")
	require.Equal(t, len(msgBytes), n3, "Error decoding messages.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestPingrespDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(PINGRESP << 4),
		0,
	}

	msg := NewPingrespMessage()
	n, err := msg.Decode(msgBytes)

	require.NoError(t, err, "Error decoding messages.")
	require.Equal(t, len(msgBytes), n, "Error decoding messages.")

	dst := make([]byte, 100)
	n2, err := msg.Encode(dst)

	require.NoError(t, err, "Error decoding messages.")
	require.Equal(t, len(msgBytes), n2, "Error decoding messages.")
	require.Equal(t, msgBytes, dst[:n2], "Error decoding messages.")

	n3, err := msg.Decode(dst)

	require.NoError(t, err, "Error decoding messages.")
	require.Equal(t, len(msgBytes), n3, "Error decoding messages.")
}
