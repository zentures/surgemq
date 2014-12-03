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

func TestConnectMessageFields(t *testing.T) {
	msg := NewConnectMessage()

	err := msg.SetVersion(0x3)
	assert.NoError(t, false, err, "Error setting message version.")

	assert.Equal(t, false, 0x3, msg.Version(), "Incorrect version number")

	err = msg.SetVersion(0x5)
	assert.Error(t, false, err)

	msg.SetCleanSession(true)
	assert.True(t, false, msg.CleanSession(), "Error setting clean session flag.")

	msg.SetCleanSession(false)
	assert.False(t, false, msg.CleanSession(), "Error setting clean session flag.")

	msg.SetWillFlag(true)
	assert.True(t, false, msg.WillFlag(), "Error setting will flag.")

	msg.SetWillFlag(false)
	assert.False(t, false, msg.WillFlag(), "Error setting will flag.")

	msg.SetWillRetain(true)
	assert.True(t, false, msg.WillRetain(), "Error setting will retain.")

	msg.SetWillRetain(false)
	assert.False(t, false, msg.WillRetain(), "Error setting will retain.")

	msg.SetPasswordFlag(true)
	assert.True(t, false, msg.PasswordFlag(), "Error setting password flag.")

	msg.SetPasswordFlag(false)
	assert.False(t, false, msg.PasswordFlag(), "Error setting password flag.")

	msg.SetUsernameFlag(true)
	assert.True(t, false, msg.UsernameFlag(), "Error setting username flag.")

	msg.SetUsernameFlag(false)
	assert.False(t, false, msg.UsernameFlag(), "Error setting username flag.")

	msg.SetWillQos(1)
	assert.Equal(t, false, 1, msg.WillQos(), "Error setting will QoS.")

	err = msg.SetWillQos(4)
	assert.Error(t, false, err)

	err = msg.SetClientId([]byte("j0j0jfajf02j0asdjf"))
	assert.NoError(t, false, err, "Error setting client ID")

	assert.Equal(t, false, "j0j0jfajf02j0asdjf", string(msg.ClientId()), "Error setting client ID.")

	err = msg.SetClientId([]byte("this is good for v3"))
	assert.NoError(t, false, err)

	msg.SetVersion(0x4)

	err = msg.SetClientId([]byte("this is no good for v4!"))
	assert.Error(t, false, err)

	msg.SetVersion(0x3)

	msg.SetWillTopic([]byte("willtopic"))
	assert.Equal(t, false, "willtopic", string(msg.WillTopic()), "Error setting will topic.")

	assert.True(t, false, msg.WillFlag(), "Error setting will flag.")

	msg.SetWillTopic([]byte(""))
	assert.Equal(t, false, "", string(msg.WillTopic()), "Error setting will topic.")

	assert.False(t, false, msg.WillFlag(), "Error setting will flag.")

	msg.SetWillMessage([]byte("this is a will message"))
	assert.Equal(t, false, "this is a will message", string(msg.WillMessage()), "Error setting will message.")

	assert.True(t, false, msg.WillFlag(), "Error setting will flag.")

	msg.SetWillMessage([]byte(""))
	assert.Equal(t, false, "", string(msg.WillMessage()), "Error setting will topic.")

	assert.False(t, false, msg.WillFlag(), "Error setting will flag.")

	msg.SetWillTopic([]byte("willtopic"))
	msg.SetWillMessage([]byte("this is a will message"))
	msg.SetWillTopic([]byte(""))
	assert.True(t, false, msg.WillFlag(), "Error setting will topic.")

	msg.SetUsername([]byte("myname"))
	assert.Equal(t, false, "myname", string(msg.Username()), "Error setting will message.")

	assert.True(t, false, msg.UsernameFlag(), "Error setting will flag.")

	msg.SetUsername([]byte(""))
	assert.Equal(t, false, "", string(msg.Username()), "Error setting will message.")

	assert.False(t, false, msg.UsernameFlag(), "Error setting will flag.")

	msg.SetPassword([]byte("myname"))
	assert.Equal(t, false, "myname", string(msg.Password()), "Error setting will message.")

	assert.True(t, false, msg.PasswordFlag(), "Error setting will flag.")

	msg.SetPassword([]byte(""))
	assert.Equal(t, false, "", string(msg.Password()), "Error setting will message.")

	assert.False(t, false, msg.PasswordFlag(), "Error setting will flag.")
}

func TestConnectMessageDecode(t *testing.T) {
	msgBytes := []byte{
		byte(CONNECT << 4),
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

	msg := NewConnectMessage()
	n, err := msg.Decode(msgBytes)

	assert.NoError(t, true, err, "Error decoding message.")
	assert.Equal(t, true, len(msgBytes), n, "Error decoding message.")
	assert.Equal(t, true, 206, msg.connectFlags, "Incorrect flag value.")
	assert.Equal(t, true, 10, msg.KeepAlive(), "Incorrect KeepAlive value.")
	assert.Equal(t, true, "surgemq", string(msg.ClientId()), "Incorrect client ID value.")
	assert.Equal(t, true, "will", string(msg.WillTopic()), "Incorrect will topic value.")
	assert.Equal(t, true, "send me home", string(msg.WillMessage()), "Incorrect will message value.")
	assert.Equal(t, true, "surgemq", string(msg.Username()), "Incorrect username value.")
	assert.Equal(t, true, "verysecret", string(msg.Password()), "Incorrect password value.")
}

func TestConnectMessageDecode2(t *testing.T) {
	// missing last byte 't'
	msgBytes := []byte{
		byte(CONNECT << 4),
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
		'v', 'e', 'r', 'y', 's', 'e', 'c', 'r', 'e',
	}

	msg := NewConnectMessage()
	_, err := msg.Decode(msgBytes)

	assert.Error(t, true, err)
}

func TestConnectMessageDecode3(t *testing.T) {
	// extra bytes
	msgBytes := []byte{
		byte(CONNECT << 4),
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
		'e', 'x', 't', 'r', 'a',
	}

	msg := NewConnectMessage()
	n, err := msg.Decode(msgBytes)

	assert.NoError(t, true, err)
	assert.Equal(t, true, 62, n)
}

func TestConnectMessageDecode4(t *testing.T) {
	// missing client Id, clean session == 0
	msgBytes := []byte{
		byte(CONNECT << 4),
		53,
		0, // Length MSB (0)
		4, // Length LSB (4)
		'M', 'Q', 'T', 'T',
		4,   // Protocol level 4
		204, // connect flags 11001110, will QoS = 01
		0,   // Keep Alive MSB (0)
		10,  // Keep Alive LSB (10)
		0,   // Client ID MSB (0)
		0,   // Client ID LSB (0)
		0,   // Will Topic MSB (0)
		4,   // Will Topic LSB (4)
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

	msg := NewConnectMessage()
	_, err := msg.Decode(msgBytes)

	assert.Error(t, true, err)
}

func TestConnectMessageEncode(t *testing.T) {
	msgBytes := []byte{
		byte(CONNECT << 4),
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

	msg := NewConnectMessage()
	msg.SetWillQos(1)
	msg.SetVersion(4)
	msg.SetCleanSession(true)
	msg.SetClientId([]byte("surgemq"))
	msg.SetKeepAlive(10)
	msg.SetWillTopic([]byte("will"))
	msg.SetWillMessage([]byte("send me home"))
	msg.SetUsername([]byte("surgemq"))
	msg.SetPassword([]byte("verysecret"))

	dst := make([]byte, 100)
	n, err := msg.Encode(dst)

	assert.NoError(t, true, err, "Error decoding message.")
	assert.Equal(t, true, len(msgBytes), n, "Error decoding message.")
	assert.Equal(t, true, msgBytes, dst[:n], "Error decoding message.")
}

// test to ensure encoding and decoding are the same
// decode, encode, and decode again
func TestConnectDecodeEncodeEquiv(t *testing.T) {
	msgBytes := []byte{
		byte(CONNECT << 4),
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

	msg := NewConnectMessage()
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
