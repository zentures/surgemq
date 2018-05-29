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

func TestMessageHeaderFields(t *testing.T) {
	header := &header{}

	header.SetRemainingLength(33)

	require.Equal(t, int32(33), header.RemainingLength())

	err := header.SetRemainingLength(268435456)

	require.Error(t, err)

	err = header.SetRemainingLength(-1)

	require.Error(t, err)

	err = header.SetType(RESERVED)

	require.Error(t, err)

	err = header.SetType(PUBREL)

	require.NoError(t, err)
	require.Equal(t, PUBREL, header.Type())
	require.Equal(t, "PUBREL", header.Name())
	require.Equal(t, 2, int(header.Flags()))
}

// Not enough bytes
func TestMessageHeaderDecode(t *testing.T) {
	buf := []byte{0x6f, 193, 2}
	header := &header{}

	_, err := header.decode(buf)
	require.Error(t, err)
}

// Remaining length too big
func TestMessageHeaderDecode2(t *testing.T) {
	buf := []byte{0x62, 0xff, 0xff, 0xff, 0xff}
	header := &header{}

	_, err := header.decode(buf)
	require.Error(t, err)
}

func TestMessageHeaderDecode3(t *testing.T) {
	buf := []byte{0x62, 0xff}
	header := &header{}

	_, err := header.decode(buf)
	require.Error(t, err)
}

func TestMessageHeaderDecode4(t *testing.T) {
	buf := []byte{0x62, 0xff, 0xff, 0xff, 0x7f}
	header := &header{
		mtypeflags: []byte{6<<4 | 2},
		//mtype:      6,
		//flags:      2,
	}

	n, err := header.decode(buf)

	require.Error(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, maxRemainingLength, header.RemainingLength())
}

func TestMessageHeaderDecode5(t *testing.T) {
	buf := []byte{0x62, 0xff, 0x7f}
	header := &header{
		mtypeflags: []byte{6<<4 | 2},
		//mtype:      6,
		//flags:      2,
	}

	n, err := header.decode(buf)
	require.Error(t, err)
	require.Equal(t, 3, n)
}

func TestMessageHeaderEncode1(t *testing.T) {
	header := &header{}
	headerBytes := []byte{0x62, 193, 2}

	err := header.SetType(PUBREL)

	require.NoError(t, err)

	err = header.SetRemainingLength(321)

	require.NoError(t, err)

	buf := make([]byte, 3)
	n, err := header.encode(buf)

	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, headerBytes, buf)
}

func TestMessageHeaderEncode2(t *testing.T) {
	header := &header{}

	err := header.SetType(PUBREL)
	require.NoError(t, err)

	header.remlen = 268435456

	buf := make([]byte, 5)
	_, err = header.encode(buf)

	require.Error(t, err)
}

func TestMessageHeaderEncode3(t *testing.T) {
	header := &header{}
	headerBytes := []byte{0x62, 0xff, 0xff, 0xff, 0x7f}

	err := header.SetType(PUBREL)

	require.NoError(t, err)

	err = header.SetRemainingLength(maxRemainingLength)

	require.NoError(t, err)

	buf := make([]byte, 5)
	n, err := header.encode(buf)

	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, headerBytes, buf)
}

func TestMessageHeaderEncode4(t *testing.T) {
	header := &header{
		mtypeflags: []byte{byte(RESERVED2) << 4},
		//mtype:      6,
		//flags:      2,
	}

	buf := make([]byte, 5)
	_, err := header.encode(buf)
	require.Error(t, err)
}

/*
// This test is to ensure that an empty message is at least 2 bytes long
func TestMessageHeaderEncode5(t *testing.T) {
	msg := NewPingreqMessage()

	dst, n, err := msg.encode()
	if err != nil {
		t.Errorf("Error encoding PINGREQ message: %v", err)
	} else if n != 2 {
		t.Errorf("Incorrect result. Expecting length of 2 bytes, got %d.", dst.(*bytes.Buffer).Len())
	}
}
*/
