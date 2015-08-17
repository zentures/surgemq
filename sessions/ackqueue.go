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

package sessions

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/surgemq/message"
)

var (
	errQueueFull   error = errors.New("queue full")
	errQueueEmpty  error = errors.New("queue empty")
	errWaitMessage error = errors.New("Invalid message to wait for ack")
	errAckMessage  error = errors.New("Invalid message for acking")
)

type ackmsg struct {
	// Message type of the message waiting for ack
	Mtype message.MessageType

	// Current state of the ack-waiting message
	State message.MessageType

	// Packet ID of the message. Every message that require ack'ing must have a valid
	// packet ID. Messages that have message I
	Pktid uint16

	// Slice containing the message bytes
	Msgbuf []byte

	// Slice containing the ack message bytes
	Ackbuf []byte

	// When ack cycle completes, call this function
	OnComplete interface{}
}

// Ackqueue is a growing queue implemented based on a ring buffer. As the buffer
// gets full, it will auto-grow.
//
// Ackqueue is used to store messages that are waiting for acks to come back. There
// are a few scenarios in which acks are required.
//   1. Client sends SUBSCRIBE message to server, waits for SUBACK.
//   2. Client sends UNSUBSCRIBE message to server, waits for UNSUBACK.
//   3. Client sends PUBLISH QoS 1 message to server, waits for PUBACK.
//   4. Server sends PUBLISH QoS 1 message to client, waits for PUBACK.
//   5. Client sends PUBLISH QoS 2 message to server, waits for PUBREC.
//   6. Server sends PUBREC message to client, waits for PUBREL.
//   7. Client sends PUBREL message to server, waits for PUBCOMP.
//   8. Server sends PUBLISH QoS 2 message to client, waits for PUBREC.
//   9. Client sends PUBREC message to server, waits for PUBREL.
//   10. Server sends PUBREL message to client, waits for PUBCOMP.
//   11. Client sends PINGREQ message to server, waits for PINGRESP.
type Ackqueue struct {
	size  int64
	mask  int64
	count int64
	head  int64
	tail  int64

	ping ackmsg
	ring []ackmsg
	emap map[uint16]int64

	ackdone []ackmsg

	mu sync.Mutex
}

func newAckqueue(n int) *Ackqueue {
	m := int64(n)
	if !powerOfTwo64(m) {
		m = roundUpPowerOfTwo64(m)
	}

	return &Ackqueue{
		size:    m,
		mask:    m - 1,
		count:   0,
		head:    0,
		tail:    0,
		ring:    make([]ackmsg, m),
		emap:    make(map[uint16]int64, m),
		ackdone: make([]ackmsg, 0),
	}
}

// Wait() copies the message into a waiting queue, and waits for the corresponding
// ack message to be received.
func (this *Ackqueue) Wait(msg message.Message, onComplete interface{}) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	switch msg := msg.(type) {
	case *message.PublishMessage:
		if msg.QoS() == message.QosAtMostOnce {
			//return fmt.Errorf("QoS 0 messages don't require ack")
			return errWaitMessage
		}

		this.insert(msg.PacketId(), msg, onComplete)

	case *message.SubscribeMessage:
		this.insert(msg.PacketId(), msg, onComplete)

	case *message.UnsubscribeMessage:
		this.insert(msg.PacketId(), msg, onComplete)

	case *message.PingreqMessage:
		this.ping = ackmsg{
			Mtype:      message.PINGREQ,
			State:      message.RESERVED,
			OnComplete: onComplete,
		}

	default:
		return errWaitMessage
	}

	return nil
}

// Ack() takes the ack message supplied and updates the status of messages waiting.
func (this *Ackqueue) Ack(msg message.Message) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	switch msg.Type() {
	case message.PUBACK, message.PUBREC, message.PUBREL, message.PUBCOMP, message.SUBACK, message.UNSUBACK:
		// Check to see if the message w/ the same packet ID is in the queue
		i, ok := this.emap[msg.PacketId()]
		if ok {
			// If message w/ the packet ID exists, update the message state and copy
			// the ack message
			this.ring[i].State = msg.Type()

			ml := msg.Len()
			this.ring[i].Ackbuf = make([]byte, ml)

			_, err := msg.Encode(this.ring[i].Ackbuf)
			if err != nil {
				return err
			}
			//glog.Debugf("Acked: %v", msg)
			//} else {
			//glog.Debugf("Cannot ack %s message with packet ID %d", msg.Type(), msg.PacketId())
		}

	case message.PINGRESP:
		if this.ping.Mtype == message.PINGREQ {
			this.ping.State = message.PINGRESP
		}

	default:
		return errAckMessage
	}

	return nil
}

// Acked() returns the list of messages that have completed the ack cycle.
func (this *Ackqueue) Acked() []ackmsg {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.ackdone = this.ackdone[0:0]

	if this.ping.State == message.PINGRESP {
		this.ackdone = append(this.ackdone, this.ping)
		this.ping = ackmsg{}
	}

FORNOTEMPTY:
	for !this.empty() {
		switch this.ring[this.head].State {
		case message.PUBACK, message.PUBREL, message.PUBCOMP, message.SUBACK, message.UNSUBACK:
			this.ackdone = append(this.ackdone, this.ring[this.head])
			this.removeHead()

		default:
			break FORNOTEMPTY
		}
	}

	return this.ackdone
}

func (this *Ackqueue) insert(pktid uint16, msg message.Message, onComplete interface{}) error {
	if this.full() {
		this.grow()
	}

	if _, ok := this.emap[pktid]; !ok {
		// message length
		ml := msg.Len()

		// ackmsg
		am := ackmsg{
			Mtype:      msg.Type(),
			State:      message.RESERVED,
			Pktid:      msg.PacketId(),
			Msgbuf:     make([]byte, ml),
			OnComplete: onComplete,
		}

		if _, err := msg.Encode(am.Msgbuf); err != nil {
			return err
		}

		this.ring[this.tail] = am
		this.emap[pktid] = this.tail
		this.tail = this.increment(this.tail)
		this.count++
	} else {
		// If packet w/ pktid already exist, then this must be a PUBLISH message
		// Other message types should never send with the same packet ID
		pm, ok := msg.(*message.PublishMessage)
		if !ok {
			return fmt.Errorf("ack/insert: duplicate packet ID for %s message", msg.Name())
		}

		// If this is a publish message, then the DUP flag must be set. This is the
		// only scenario in which we will receive duplicate messages.
		if pm.Dup() {
			return fmt.Errorf("ack/insert: duplicate packet ID for PUBLISH message, but DUP flag is not set")
		}

		// Since it's a dup, there's really nothing we need to do. Moving on...
	}

	return nil
}

func (this *Ackqueue) removeHead() error {
	if this.empty() {
		return errQueueEmpty
	}

	it := this.ring[this.head]
	// set this to empty ackmsg{} to ensure GC will collect the buffer
	this.ring[this.head] = ackmsg{}
	this.head = this.increment(this.head)
	this.count--
	delete(this.emap, it.Pktid)

	return nil
}

func (this *Ackqueue) grow() {
	if math.MaxInt64/2 < this.size {
		panic("new size will overflow int64")
	}

	newsize := this.size << 1
	newmask := newsize - 1
	newring := make([]ackmsg, newsize)

	if this.tail > this.head {
		copy(newring, this.ring[this.head:this.tail])
	} else {
		copy(newring, this.ring[this.head:])
		copy(newring[this.size-this.head:], this.ring[:this.tail])
	}

	this.size = newsize
	this.mask = newmask
	this.ring = newring
	this.head = 0
	this.tail = this.count

	this.emap = make(map[uint16]int64, this.size)

	for i := int64(0); i < this.tail; i++ {
		this.emap[this.ring[i].Pktid] = i
	}
}

func (this *Ackqueue) len() int {
	return int(this.count)
}

func (this *Ackqueue) cap() int {
	return int(this.size)
}

func (this *Ackqueue) index(n int64) int64 {
	return n & this.mask
}

func (this *Ackqueue) full() bool {
	return this.count == this.size
}

func (this *Ackqueue) empty() bool {
	return this.count == 0
}

func (this *Ackqueue) increment(n int64) int64 {
	return this.index(n + 1)
}

func powerOfTwo64(n int64) bool {
	return n != 0 && (n&(n-1)) == 0
}

func roundUpPowerOfTwo64(n int64) int64 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++

	return n
}
