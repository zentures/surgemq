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
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	maxLPString          uint16 = 65535
	maxFixedHeaderLength int    = 5
	maxRemainingLength   int32  = 268435455 // bytes, or 256 MB
)

const (
	// QoS 0: At most once delivery
	// The message is delivered according to the capabilities of the underlying network.
	// No response is sent by the receiver and no retry is performed by the sender. The
	// message arrives at the receiver either once or not at all.
	QosAtMostOnce byte = iota

	// QoS 1: At least once delivery
	// This quality of service ensures that the message arrives at the receiver at least once.
	// A QoS 1 PUBLISH Packet has a Packet Identifier in its variable header and is acknowledged
	// by a PUBACK Packet. Section 2.3.1 provides more information about Packet Identifiers.
	QosAtLeastOnce

	// QoS 2: Exactly once delivery
	// This is the highest quality of service, for use when neither loss nor duplication of
	// messages are acceptable. There is an increased overhead associated with this quality of
	// service.
	QosExactlyOnce

	// QosFailure is a return value for a subscription if there's a problem while subscribing
	// to a specific topic.
	QosFailure = 0x80
)

// SupportedVersions is a map of the version number (0x3 or 0x4) to the version string,
// "MQIsdp" for 0x3, and "MQTT" for 0x4.
var SupportedVersions map[byte]string = map[byte]string{
	0x3: "MQIsdp",
	0x4: "MQTT",
}

// MessageType is the type representing the MQTT packet types. In the MQTT spec,
// MQTT control packet type is represented as a 4-bit unsigned value.
type MessageType byte

// Message is an interface defined for all MQTT message types.
type Message interface {
	// Name returns a string representation of the message type. Examples include
	// "PUBLISH", "SUBSCRIBE", and others. This is statically defined for each of
	// the message types and cannot be changed.
	Name() string

	// Desc returns a string description of the message type. For example, a
	// CONNECT message would return "Client request to connect to Server." These
	// descriptions are statically defined (copied from the MQTT spec) and cannot
	// be changed.
	Desc() string

	// Type returns the MessageType of the Message. The retured value should be one
	// of the constants defined for MessageType.
	Type() MessageType

	// PacketId returns the packet ID of the Message. The retured value is 0 if
	// there's no packet ID for this message type. Otherwise non-0.
	PacketId() uint16

	// Encode writes the message bytes into the byte array from the argument. It
	// returns the number of bytes encoded and whether there's any errors along
	// the way. If there's any errors, then the byte slice and count should be
	// considered invalid.
	Encode([]byte) (int, error)

	// Decode reads the bytes in the byte slice from the argument. It returns the
	// total number of bytes decoded, and whether there's any errors during the
	// process. The byte slice MUST NOT be modified during the duration of this
	// message being available since the byte slice is internally stored for
	// references.
	Decode([]byte) (int, error)

	Len() int
}

const (
	// RESERVED is a reserved value and should be considered an invalid message type
	RESERVED MessageType = iota

	// CONNECT: Client to Server. Client request to connect to Server.
	CONNECT

	// CONNACK: Server to Client. Connect acknowledgement.
	CONNACK

	// PUBLISH: Client to Server, or Server to Client. Publish message.
	PUBLISH

	// PUBACK: Client to Server, or Server to Client. Publish acknowledgment for
	// QoS 1 messages.
	PUBACK

	// PUBACK: Client to Server, or Server to Client. Publish received for QoS 2 messages.
	// Assured delivery part 1.
	PUBREC

	// PUBREL: Client to Server, or Server to Client. Publish release for QoS 2 messages.
	// Assured delivery part 1.
	PUBREL

	// PUBCOMP: Client to Server, or Server to Client. Publish complete for QoS 2 messages.
	// Assured delivery part 3.
	PUBCOMP

	// SUBSCRIBE: Client to Server. Client subscribe request.
	SUBSCRIBE

	// SUBACK: Server to Client. Subscribe acknowledgement.
	SUBACK

	// UNSUBSCRIBE: Client to Server. Unsubscribe request.
	UNSUBSCRIBE

	// UNSUBACK: Server to Client. Unsubscribe acknowlegment.
	UNSUBACK

	// PINGREQ: Client to Server. PING request.
	PINGREQ

	// PINGRESP: Server to Client. PING response.
	PINGRESP

	// DISCONNECT: Client to Server. Client is disconnecting.
	DISCONNECT

	// RESERVED2 is a reserved value and should be considered an invalid message type.
	RESERVED2
)

func (this MessageType) String() string {
	return this.Name()
}

// Name returns the name of the message type. It should correspond to one of the
// constant values defined for MessageType. It is statically defined and cannot
// be changed.
func (this MessageType) Name() string {
	switch this {
	case RESERVED:
		return "RESERVED"
	case CONNECT:
		return "CONNECT"
	case CONNACK:
		return "CONNACK"
	case PUBLISH:
		return "PUBLISH"
	case PUBACK:
		return "PUBACK"
	case PUBREC:
		return "PUBREC"
	case PUBREL:
		return "PUBREL"
	case PUBCOMP:
		return "PUBCOMP"
	case SUBSCRIBE:
		return "SUBSCRIBE"
	case SUBACK:
		return "SUBACK"
	case UNSUBSCRIBE:
		return "UNSUBSCRIBE"
	case UNSUBACK:
		return "UNSUBACK"
	case PINGREQ:
		return "PINGREQ"
	case PINGRESP:
		return "PINGRESP"
	case DISCONNECT:
		return "DISCONNECT"
	case RESERVED2:
		return "RESERVED2"
	}

	return "UNKNOWN"
}

// Desc returns the description of the message type. It is statically defined (copied
// from MQTT spec) and cannot be changed.
func (this MessageType) Desc() string {
	switch this {
	case RESERVED:
		return "Reserved"
	case CONNECT:
		return "Client request to connect to Server"
	case CONNACK:
		return "Connect acknowledgement"
	case PUBLISH:
		return "Publish message"
	case PUBACK:
		return "Publish acknowledgement"
	case PUBREC:
		return "Publish received (assured delivery part 1)"
	case PUBREL:
		return "Publish release (assured delivery part 2)"
	case PUBCOMP:
		return "Publish complete (assured delivery part 3)"
	case SUBSCRIBE:
		return "Client subscribe request"
	case SUBACK:
		return "Subscribe acknowledgement"
	case UNSUBSCRIBE:
		return "Unsubscribe request"
	case UNSUBACK:
		return "Unsubscribe acknowledgement"
	case PINGREQ:
		return "PING request"
	case PINGRESP:
		return "PING response"
	case DISCONNECT:
		return "Client is disconnecting"
	case RESERVED2:
		return "Reserved"
	}

	return "UNKNOWN"
}

// DefaultFlags returns the default flag values for the message type, as defined by
// the MQTT spec.
func (this MessageType) DefaultFlags() byte {
	switch this {
	case RESERVED:
		return 0
	case CONNECT:
		return 0
	case CONNACK:
		return 0
	case PUBLISH:
		return 0
	case PUBACK:
		return 0
	case PUBREC:
		return 0
	case PUBREL:
		return 2
	case PUBCOMP:
		return 0
	case SUBSCRIBE:
		return 2
	case SUBACK:
		return 0
	case UNSUBSCRIBE:
		return 2
	case UNSUBACK:
		return 0
	case PINGREQ:
		return 0
	case PINGRESP:
		return 0
	case DISCONNECT:
		return 0
	case RESERVED2:
		return 0
	}

	return 0
}

// New creates a new message based on the message type. It is a shortcut to call
// one of the New*Message functions. If an error is returned then the message type
// is invalid.
func (this MessageType) New() (Message, error) {
	switch this {
	case CONNECT:
		return NewConnectMessage(), nil
	case CONNACK:
		return NewConnackMessage(), nil
	case PUBLISH:
		return NewPublishMessage(), nil
	case PUBACK:
		return NewPubackMessage(), nil
	case PUBREC:
		return NewPubrecMessage(), nil
	case PUBREL:
		return NewPubrelMessage(), nil
	case PUBCOMP:
		return NewPubcompMessage(), nil
	case SUBSCRIBE:
		return NewSubscribeMessage(), nil
	case SUBACK:
		return NewSubackMessage(), nil
	case UNSUBSCRIBE:
		return NewUnsubscribeMessage(), nil
	case UNSUBACK:
		return NewUnsubackMessage(), nil
	case PINGREQ:
		return NewPingreqMessage(), nil
	case PINGRESP:
		return NewPingrespMessage(), nil
	case DISCONNECT:
		return NewDisconnectMessage(), nil
	}

	return nil, fmt.Errorf("msgtype/NewMessage: Invalid message type %d", this)
}

// Valid returns a boolean indicating whether the message type is valid or not.
func (this MessageType) Valid() bool {
	return this > RESERVED && this < RESERVED2
}

// ValidTopic checks the topic, which is a slice of bytes, to see if it's valid. Topic is
// considered valid if it's longer than 0 bytes, and doesn't contain any wildcard characters
// such as + and #.
func ValidTopic(topic []byte) bool {
	return len(topic) > 0 && bytes.IndexByte(topic, '#') == -1 && bytes.IndexByte(topic, '+') == -1
}

// ValidQos checks the QoS value to see if it's valid. Valid QoS are QosAtMostOnce,
// QosAtLeastonce, and QosExactlyOnce.
func ValidQos(qos byte) bool {
	return qos == QosAtMostOnce || qos == QosAtLeastOnce || qos == QosExactlyOnce
}

// ValidVersion checks to see if the version is valid. Current supported versions include 0x3 and 0x4.
func ValidVersion(v byte) bool {
	_, ok := SupportedVersions[v]
	return ok
}

// ValidConnackError checks to see if the error is a Connack Error or not
func ValidConnackError(err error) bool {
	return err == ErrInvalidProtocolVersion || err == ErrIdentifierRejected ||
		err == ErrServerUnavailable || err == ErrBadUsernameOrPassword || err == ErrNotAuthorized
}

// Read length prefixed bytes
func readLPBytes(buf []byte) ([]byte, int, error) {
	if len(buf) < 2 {
		return nil, 0, fmt.Errorf("utils/readLPBytes: Insufficient buffer size. Expecting %d, got %d.", 2, len(buf))
	}

	n, total := 0, 0

	n = int(binary.BigEndian.Uint16(buf))
	total += 2

	if len(buf) < n {
		return nil, total, fmt.Errorf("utils/readLPBytes: Insufficient buffer size. Expecting %d, got %d.", n, len(buf))
	}

	total += n

	return buf[2:total], total, nil
}

// Write length prefixed bytes
func writeLPBytes(buf []byte, b []byte) (int, error) {
	total, n := 0, len(b)

	if n > int(maxLPString) {
		return 0, fmt.Errorf("utils/writeLPBytes: Length (%d) greater than %d bytes.", n, maxLPString)
	}

	if len(buf) < 2+n {
		return 0, fmt.Errorf("utils/writeLPBytes: Insufficient buffer size. Expecting %d, got %d.", 2+n, len(buf))
	}

	binary.BigEndian.PutUint16(buf, uint16(n))
	total += 2

	copy(buf[total:], b)
	total += n

	return total, nil
}
