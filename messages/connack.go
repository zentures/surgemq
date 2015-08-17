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

import "fmt"

// The CONNACK Packet is the packet sent by the Server in response to a CONNECT Packet
// received from a Client. The first packet sent from the Server to the Client MUST
// be a CONNACK Packet [MQTT-3.2.0-1].
//
// If the Client does not receive a CONNACK Packet from the Server within a reasonable
// amount of time, the Client SHOULD close the Network Connection. A "reasonable" amount
// of time depends on the type of application and the communications infrastructure.
type ConnackMessage struct {
	header

	sessionPresent bool
	returnCode     ConnackCode
}

var _ Message = (*ConnackMessage)(nil)

// NewConnackMessage creates a new CONNACK message
func NewConnackMessage() *ConnackMessage {
	msg := &ConnackMessage{}
	msg.SetType(CONNACK)

	return msg
}

// String returns a string representation of the CONNACK message
func (this ConnackMessage) String() string {
	return fmt.Sprintf("%s, Session Present=%t, Return code=%q\n", this.header, this.sessionPresent, this.returnCode)
}

// SessionPresent returns the session present flag value
func (this *ConnackMessage) SessionPresent() bool {
	return this.sessionPresent
}

// SetSessionPresent sets the value of the session present flag
func (this *ConnackMessage) SetSessionPresent(v bool) {
	if v {
		this.sessionPresent = true
	} else {
		this.sessionPresent = false
	}

	this.dirty = true
}

// ReturnCode returns the return code received for the CONNECT message. The return
// type is an error
func (this *ConnackMessage) ReturnCode() ConnackCode {
	return this.returnCode
}

func (this *ConnackMessage) SetReturnCode(ret ConnackCode) {
	this.returnCode = ret
	this.dirty = true
}

func (this *ConnackMessage) Len() int {
	if !this.dirty {
		return len(this.dbuf)
	}

	ml := this.msglen()

	if err := this.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return this.header.msglen() + ml
}

func (this *ConnackMessage) Decode(src []byte) (int, error) {
	total := 0

	n, err := this.header.decode(src)
	total += n
	if err != nil {
		return total, err
	}

	b := src[total]

	if b&254 != 0 {
		return 0, fmt.Errorf("connack/Decode: Bits 7-1 in Connack Acknowledge Flags byte (1) are not 0")
	}

	this.sessionPresent = b&0x1 == 1
	total++

	b = src[total]

	// Read return code
	if b > 5 {
		return 0, fmt.Errorf("connack/Decode: Invalid CONNACK return code (%d)", b)
	}

	this.returnCode = ConnackCode(b)
	total++

	this.dirty = false

	return total, nil
}

func (this *ConnackMessage) Encode(dst []byte) (int, error) {
	if !this.dirty {
		if len(dst) < len(this.dbuf) {
			return 0, fmt.Errorf("connack/Encode: Insufficient buffer size. Expecting %d, got %d.", len(this.dbuf), len(dst))
		}

		return copy(dst, this.dbuf), nil
	}

	// CONNACK remaining length fixed at 2 bytes
	hl := this.header.msglen()
	ml := this.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("connack/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	if err := this.SetRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := this.header.encode(dst[total:])
	total += n
	if err != nil {
		return 0, err
	}

	if this.sessionPresent {
		dst[total] = 1
	}
	total++

	if this.returnCode > 5 {
		return total, fmt.Errorf("connack/Encode: Invalid CONNACK return code (%d)", this.returnCode)
	}

	dst[total] = this.returnCode.Value()
	total++

	return total, nil
}

func (this *ConnackMessage) msglen() int {
	return 2
}
