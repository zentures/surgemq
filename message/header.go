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
	"encoding/binary"
	"fmt"
)

var (
	gPacketId uint64 = 0
)

// Fixed header
// - 1 byte for control packet type (bits 7-4) and flags (bits 3-0)
// - up to 4 byte for remaining length
type header struct {
	remlen   int32
	mtype    MessageType
	flags    byte
	packetId uint16
}

// String returns a string representation of the message.
func (this header) String() string {
	return fmt.Sprintf("Packet type=%s, Flags=%08b, Remaining Length=%d bytes", this.mtype.Name(), this.flags, this.remlen)
}

// Name returns a string representation of the message type. Examples include
// "PUBLISH", "SUBSCRIBE", and others. This is statically defined for each of
// the message types and cannot be changed.
func (this *header) Name() string {
	return this.Type().Name()
}

// Desc returns a string description of the message type. For example, a
// CONNECT message would return "Client request to connect to Server." These
// descriptions are statically defined (copied from the MQTT spec) and cannot
// be changed.
func (this *header) Desc() string {
	return this.Type().Desc()
}

// Type returns the MessageType of the Message. The retured value should be one
// of the constants defined for MessageType.
func (this *header) Type() MessageType {
	return this.mtype
}

// SetType sets the message type of this message. It also correctly sets the
// default flags for the message type. It returns an error if the type is invalid.
func (this *header) SetType(mtype MessageType) error {
	if !mtype.Valid() {
		return fmt.Errorf("header/SetType: Invalid control packet type %d", mtype)
	}

	this.mtype = mtype

	this.flags = mtype.DefaultFlags()

	return nil
}

// Flags returns the fixed header flags for this message.
func (this *header) Flags() byte {
	return this.flags
}

// RemainingLength returns the length of the non-fixed-header part of the message.
func (this *header) RemainingLength() int32 {
	return this.remlen
}

// SetRemainingLength sets the length of the non-fixed-header part of the message.
// It returns error if the length is greater than 268435455, which is the max
// message length as defined by the MQTT spec.
func (this *header) SetRemainingLength(remlen int32) error {
	if remlen > maxRemainingLength || remlen < 0 {
		return fmt.Errorf("header/SetLength: Remaining length (%d) out of bound (max %d, min 0)", remlen, maxRemainingLength)
	}

	this.remlen = remlen
	return nil
}

func (this *header) Len() int {
	return this.msglen()
}

// PacketId returns the ID of the packet.
func (this *header) PacketId() uint16 {
	return this.packetId
}

// SetPacketId sets the ID of the packet.
func (this *header) SetPacketId(v uint16) {
	this.packetId = v
}

func (this *header) encode(dst []byte) (int, error) {
	ml := this.msglen()

	if len(dst) < ml {
		return 0, fmt.Errorf("header/Encode: Insufficient buffer size. Expecting %d, got %d.", ml, len(dst))
	}

	total := 0

	if this.remlen > maxRemainingLength || this.remlen < 0 {
		return total, fmt.Errorf("header/Encode: Remaining length (%d) out of bound (max %d, min 0)", this.remlen, maxRemainingLength)
	}

	if !this.mtype.Valid() {
		return total, fmt.Errorf("header/Encode: Invalid message type %d", this.mtype)
	}

	dst[total] = byte(this.mtype)<<4 | this.flags
	total += 1

	n := binary.PutUvarint(dst[total:], uint64(this.remlen))
	total += n

	return total, nil
}

// Decode reads from the io.Reader parameter until a full message is decoded, or
// when io.Reader returns EOF or error. The first return value is the number of
// bytes read from io.Reader. The second is error if Decode encounters any problems.
func (this *header) decode(src []byte) (int, error) {
	total := 0

	mtype := MessageType(src[total] >> 4)
	if !mtype.Valid() {
		return total, fmt.Errorf("header/Decode: Invalid message type %d.", mtype)
	}

	if mtype != this.mtype {
		return total, fmt.Errorf("header/Decode: Invalid message type %d. Expecting %d.", mtype, this.mtype)
	}

	this.flags = src[total] & 0x0f
	if this.mtype != PUBLISH && this.flags != this.mtype.DefaultFlags() {
		return total, fmt.Errorf("header/Decode: Invalid message (%d) flags. Expecting %d, got %d", this.mtype, this.mtype.DefaultFlags, this.flags)
	}

	if this.mtype == PUBLISH && !ValidQos((this.flags>>1)&0x3) {
		return total, fmt.Errorf("header/Decode: Invalid QoS (%d) for PUBLISH message.", (this.flags>>1)&0x3)
	}

	total++

	remlen, m := binary.Uvarint(src[total:])
	total += m
	this.remlen = int32(remlen)

	if this.remlen > maxRemainingLength || remlen < 0 {
		return total, fmt.Errorf("header/Decode: Remaining length (%d) out of bound (max %d, min 0)", this.remlen, maxRemainingLength)
	}

	if int(this.remlen) > len(src[total:]) {
		return total, fmt.Errorf("header/Decode: Remaining length (%d) is greater than remaining buffer (%d)", this.remlen, len(src[total:]))
	}

	return total, nil
}

func (this *header) msglen() int {
	// message type and flag byte
	total := 1

	if this.remlen <= 127 {
		total += 1
	} else if this.remlen <= 16383 {
		total += 2
	} else if this.remlen <= 2097151 {
		total += 3
	} else {
		total += 4
	}

	return total
}
