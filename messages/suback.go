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

// A SUBACK Packet is sent by the Server to the Client to confirm receipt and processing
// of a SUBSCRIBE Packet.
//
// A SUBACK Packet contains a list of return codes, that specify the maximum QoS level
// that was granted in each Subscription that was requested by the SUBSCRIBE.
type SubackMessage struct {
	header

	returnCodes []byte
}

var _ Message = (*SubackMessage)(nil)

// NewSubackMessage creates a new SUBACK message.
func NewSubackMessage() *SubackMessage {
	msg := &SubackMessage{}
	msg.SetType(SUBACK)

	return msg
}

// String returns a string representation of the message.
func (this SubackMessage) String() string {
	return fmt.Sprintf("%s, Packet ID=%d, Return Codes=%v", this.header, this.PacketId(), this.returnCodes)
}

// ReturnCodes returns the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
func (this *SubackMessage) ReturnCodes() []byte {
	return this.returnCodes
}

// AddReturnCodes sets the list of QoS returns from the subscriptions sent in the SUBSCRIBE message.
// An error is returned if any of the QoS values are not valid.
func (this *SubackMessage) AddReturnCodes(ret []byte) error {
	for _, c := range ret {
		if c != QosAtMostOnce && c != QosAtLeastOnce && c != QosExactlyOnce && c != QosFailure {
			return fmt.Errorf("suback/AddReturnCode: Invalid return code %d. Must be 0, 1, 2, 0x80.", c)
		}

		this.returnCodes = append(this.returnCodes, c)
	}

	this.dirty = true

	return nil
}

// AddReturnCode adds a single QoS return value.
func (this *SubackMessage) AddReturnCode(ret byte) error {
	return this.AddReturnCodes([]byte{ret})
}

func (this *SubackMessage) Len() int {
	if !this.dirty {
		return len(this.dbuf)
	}

	ml := this.msglen()

	if err := this.SetRemainingLength(int32(ml)); err != nil {
		return 0
	}

	return this.header.msglen() + ml
}

func (this *SubackMessage) Decode(src []byte) (int, error) {
	total := 0

	hn, err := this.header.decode(src[total:])
	total += hn
	if err != nil {
		return total, err
	}

	//this.packetId = binary.BigEndian.Uint16(src[total:])
	this.packetId = src[total : total+2]
	total += 2

	l := int(this.remlen) - (total - hn)
	this.returnCodes = src[total : total+l]
	total += len(this.returnCodes)

	for i, code := range this.returnCodes {
		if code != 0x00 && code != 0x01 && code != 0x02 && code != 0x80 {
			return total, fmt.Errorf("suback/Decode: Invalid return code %d for topic %d", code, i)
		}
	}

	this.dirty = false

	return total, nil
}

func (this *SubackMessage) Encode(dst []byte) (int, error) {
	if !this.dirty {
		if len(dst) < len(this.dbuf) {
			return 0, fmt.Errorf("suback/Encode: Insufficient buffer size. Expecting %d, got %d.", len(this.dbuf), len(dst))
		}

		return copy(dst, this.dbuf), nil
	}

	for i, code := range this.returnCodes {
		if code != 0x00 && code != 0x01 && code != 0x02 && code != 0x80 {
			return 0, fmt.Errorf("suback/Encode: Invalid return code %d for topic %d", code, i)
		}
	}

	hl := this.header.msglen()
	ml := this.msglen()

	if len(dst) < hl+ml {
		return 0, fmt.Errorf("suback/Encode: Insufficient buffer size. Expecting %d, got %d.", hl+ml, len(dst))
	}

	if err := this.SetRemainingLength(int32(ml)); err != nil {
		return 0, err
	}

	total := 0

	n, err := this.header.encode(dst[total:])
	total += n
	if err != nil {
		return total, err
	}

	if copy(dst[total:total+2], this.packetId) != 2 {
		dst[total], dst[total+1] = 0, 0
	}
	total += 2

	copy(dst[total:], this.returnCodes)
	total += len(this.returnCodes)

	return total, nil
}

func (this *SubackMessage) msglen() int {
	return 2 + len(this.returnCodes)
}
