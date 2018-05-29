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
	"fmt"
	"sync/atomic"
)

// A PUBLISH Control Packet is sent from a Client to a Server or from Server to a Client
// to transport an Application Message.
type PublishMessage struct {
	header

	topic   []byte
	payload []byte
}

var _ Message = (*PublishMessage)(nil)

// NewPublishMessage creates a new PUBLISH message.
func NewPublishMessage() *PublishMessage {
	msg := &PublishMessage{}
	msg.SetType(PUBLISH)

	return msg
}

func (this PublishMessage) String() string {
	return fmt.Sprintf("%s, Topic=%q, Packet ID=%d, QoS=%d, Retained=%t, Dup=%t, Payload=%v",
		this.header, this.topic, this.packetId, this.QoS(), this.Retain(), this.Dup(), this.payload)
}

// Dup returns the value specifying the duplicate delivery of a PUBLISH Control Packet.
// If the DUP flag is set to 0, it indicates that this is the first occasion that the
// Client or Server has attempted to send this MQTT PUBLISH Packet. If the DUP flag is
// set to 1, it indicates that this might be re-delivery of an earlier attempt to send
// the Packet.
func (this *PublishMessage) Dup() bool {
	return ((this.Flags() >> 3) & 0x1) == 1
}

// SetDup sets the value specifying the duplicate delivery of a PUBLISH Control Packet.
func (this *PublishMessage) SetDup(v bool) {
	if v {
		this.mtypeflags[0] |= 0x8 // 00001000
	} else {
		this.mtypeflags[0] &= 247 // 11110111
	}
}

// Retain returns the value of the RETAIN flag. This flag is only used on the PUBLISH
// Packet. If the RETAIN flag is set to 1, in a PUBLISH Packet sent by a Client to a
// Server, the Server MUST store the Application Message and its QoS, so that it can be
// delivered to future subscribers whose subscriptions match its topic name.
func (this *PublishMessage) Retain() bool {
	return (this.Flags() & 0x1) == 1
}

// SetRetain sets the value of the RETAIN flag.
func (this *PublishMessage) SetRetain(v bool) {
	if v {
		this.mtypeflags[0] |= 0x1 // 00000001
	} else {
		this.mtypeflags[0] &= 254 // 11111110
	}
}

// QoS returns the field that indicates the level of assurance for delivery of an
// Application Message. The values are QosAtMostOnce, QosAtLeastOnce and QosExactlyOnce.
func (this *PublishMessage) QoS() byte {
	return (this.Flags() >> 1) & 0x3
}

// SetQoS sets the field that indicates the level of assurance for delivery of an
// Application Message. The values are QosAtMostOnce, QosAtLeastOnce and QosExactlyOnce.
// An error is returned if the value is not one of these.
func (this *PublishMessage) SetQoS(v byte) error {
	if v != 0x0 && v != 0x1 && v != 0x2 {
		return fmt.Errorf("publish/SetQoS: Invalid QoS %d.", v)
	}

	this.mtypeflags[0] = (this.mtypeflags[0] & 249) | (v << 1) // 249 = 11111001

	return nil
}

// Topic returns the the topic name that identifies the information channel to which
// payload data is published.
func (this *PublishMessage) Topic() []byte {
	return this.topic
}

// SetTopic sets the the topic name that identifies the information channel to which
// payload data is published. An error is returned if ValidTopic() is falbase.
func (this *PublishMessage) SetTopic(v []byte) error {
	if !ValidTopic(v) {
		return fmt.Errorf("publish/SetTopic: Invalid topic name (%s). Must not be empty or contain wildcard characters", string(v))
	}

	this.topic = v
	this.dirty = true

	return nil
}

// Payload returns the application message that's part of the PUBLISH message.
func (this *PublishMessage) Payload() []byte {
	return this.payload
}

// SetPayload sets the application message that's part of the PUBLISH message.
func (this *PublishMessage) SetPayload(v []byte) {
	this.payload = v
	this.dirty = true
}

func (this *PublishMessage) Len() int {
	if !this.dirty {
		return len(this.dbuf)
	}

	ml := this.msglen()

	if err := this.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return this.header.msglen() + ml
}

func (this *PublishMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := this.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	n := 0

	this.topic, n, err = readLPBytes(src[total:])
	total += n
	if err != nil {
		return total, err
	}

	if !ValidTopic(this.topic) {
		return total, fmt.Errorf("publish/Decode: Invalid topic name (%s). Must not be empty or contain wildcard characters", string(this.topic))
	}

	// The packet identifier field is only present in the PUBLISH packets where the
	// QoS level is 1 or 2
	if this.QoS() != 0 {
		//this.packetId = binary.BigEndian.Uint16(src[total:])
		this.packetId = src[total : total+2]
		total += 2
	}

	l := int(this.remlen) - (total - hn)
	this.payload = src[total : total+l]
	total += len(this.payload)

	this.dirty = false

	return total, nil
}

func (this *PublishMessage) Encode(dst []byte) (int, error) {
	if !this.dirty {
		if len(dst) < len(this.dbuf) {
			return 0, fmt.Errorf("publish/Encode: Insufficient buffer size. Expecting %d, got %d.", len(this.dbuf), len(dst))
		}

		return copy(dst, this.dbuf), nil
	}

	if len(this.topic) == 0 {
		return 0, fmt.Errorf("publish/Encode: Topic name is empty.")
	}

	if len(this.payload) == 0 {
		return 0, fmt.Errorf("publish/Encode: Payload is empty.")
	}

	ml := this.msglen()

	if err := this.SetRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	hl := this.header.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("publish/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	total := 0

	n, err := this.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	n, err = writeLPBytes(dst[total:], this.topic)
	total += n
	if err != nil {
		return total, err
	}

	// The packet identifier field is only present in the PUBLISH packets where the QoS level is 1 or 2
	if this.QoS() != 0 {
		if this.PacketId() == 0 {
			this.SetPacketId(uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff))
			//this.packetId = uint16(atomic.AddUint64(&gPacketId, 1) & 0xffff)
		}

		n = copy(dst[total:], this.packetId)
		//binary.BigEndian.PutUint16(dst[total:], this.packetId)
		total += n
	}

	copy(dst[total:], this.payload)
	total += len(this.payload)

	return total, nil
}

func (this *PublishMessage) msglen() int {
	total := 2 + len(this.topic) + len(this.payload)
	if this.QoS() != 0 {
		total += 2
	}

	return total
}
