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

package service

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/dataence/bithacks"
	"github.com/surge/surgemq/message"
)

var (
	errQueueFull      error = errors.New("queue full")
	errQueueEmpty     error = errors.New("queue empty")
	errAckWaitMessage error = errors.New("Invalid message to wait for ack")
	errAckMessage     error = errors.New("Invalid message for acking")
)

type ackmsg struct {
	msg message.Message
	ack message.Message

	pktid uint16
	state message.MessageType

	onComplete OnCompleteFunc
}

// ackqueue is a growing queue implemented based on a ring buffer. As the buffer
// gets full, it will auto-grow.
type ackqueue struct {
	size  int64
	mask  int64
	count int64
	head  int64
	tail  int64

	ring []ackmsg
	emap map[uint16]int64

	ping  ackmsg
	acked []ackmsg

	mu sync.Mutex
}

func newAckqueue(n int) *ackqueue {
	m := int64(n)
	if !bithacks.PowerOfTwo(n) {
		m = bithacks.RoundUpPowerOfTwo64(m)
	}

	return &ackqueue{
		size:  m,
		mask:  m - 1,
		count: 0,
		head:  0,
		tail:  0,
		ring:  make([]ackmsg, m),
		emap:  make(map[uint16]int64, m),
		acked: make([]ackmsg, 0),
	}
}

// Len() returns the length of the queue. However, it is likely invalid as there's
// a possibility where
// 		1. an ackmsg is removed from the queue
//		2. the ackmsg is inserted into the channel
//		3. another ackmsg is removed from the queue
// In this case, the result of Len() is n-2, but the caller maybe expecting the
// Len() to be n-1 if they just read from the channel, or the caller maybe expecting
// the Len() to be n-2 if they haven't read from the channel yet.
// TODO: Fix Len() so it's correct
func (this *ackqueue) Len() int {
	return int(this.count)
}

func (this *ackqueue) Cap() int {
	return int(this.size)
}

func (this *ackqueue) AckWait(msg message.Message, onComplete OnCompleteFunc) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	switch msg := msg.(type) {
	case *message.PublishMessage:
		if msg.QoS() == message.QosAtMostOnce {
			return fmt.Errorf("QoS 0 messages don't require ack")
		}

		this.insert(msg.PacketId(), msg, onComplete)

	case *message.SubscribeMessage:
		this.insert(msg.PacketId(), msg, onComplete)

	case *message.UnsubscribeMessage:
		this.insert(msg.PacketId(), msg, onComplete)

	case *message.PingreqMessage:
		this.ping = ackmsg{msg: msg, onComplete: onComplete}

	default:
		return errAckWaitMessage
	}

	return nil
}

func (this *ackqueue) Ack(msg message.Message) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	switch msg := msg.(type) {
	case *message.PubackMessage:
		this.ack(msg.PacketId(), msg)

	case *message.PubrecMessage:
		this.ack(msg.PacketId(), msg)

	case *message.PubrelMessage:
		this.ack(msg.PacketId(), msg)

	case *message.PubcompMessage:
		this.ack(msg.PacketId(), msg)

	case *message.SubackMessage:
		this.ack(msg.PacketId(), msg)

	case *message.UnsubackMessage:
		this.ack(msg.PacketId(), msg)

	case *message.PingrespMessage:
		if this.ping.msg != nil {
			this.ping.ack = msg
			this.ping.state = msg.Type()
		}

	default:
		return errAckMessage
	}

	return nil
}

func (this *ackqueue) Acked() []ackmsg {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.acked = this.acked[0:0]

	if this.ping.state == message.PINGRESP {
		this.acked = append(this.acked, this.ping)
		this.ping = ackmsg{}
	}

	for !this.empty() {
		state := this.ring[this.head].state

		if state == message.PUBACK || state == message.PUBREL || state == message.PUBCOMP ||
			state == message.SUBACK || state == message.UNSUBACK {

			this.acked = append(this.acked, this.ring[this.head])
			this.removeHead()
		} else {
			break
		}
	}

	return this.acked
}

func (this *ackqueue) insert(pktid uint16, msg message.Message, onComplete OnCompleteFunc) {
	if this.full() {
		this.grow()
	}

	i, ok := this.emap[pktid]
	if !ok {
		this.ring[this.tail] = ackmsg{msg: msg, pktid: pktid, onComplete: onComplete}
		this.emap[pktid] = this.tail
		this.tail = this.increment(this.tail)
		this.count++
	} else {
		this.ring[i].msg = msg
	}
}

func (this *ackqueue) ack(pktid uint16, msg message.Message) {
	//glog.Debugf("acking %s id %d", msg.Name(), pktid)

	i, ok := this.emap[pktid]
	if ok {
		//glog.Debugf("found %d in emap[%d]", pktid, i)
		this.ring[i].state = msg.Type()
		this.ring[i].ack = msg

	}
}

func (this *ackqueue) removeHead() error {
	if this.empty() {
		return errQueueEmpty
	}

	it := this.ring[this.head]
	this.ring[this.head] = ackmsg{}
	this.head = this.increment(this.head)
	this.count--
	delete(this.emap, it.pktid)

	return nil
}

func (this *ackqueue) grow() {
	if math.MaxInt64/2 < this.size {
		panic("new size will overflow int64")
	}

	newsize := this.size << 1
	newmask := newsize - 1
	newring := make([]ackmsg, newsize)

	//glog.Debugf("newsize = %d", newsize)
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
		this.emap[this.ring[i].pktid] = i
	}
}

func (this *ackqueue) index(n int64) int64 {
	return n & this.mask
}

func (this *ackqueue) full() bool {
	return this.count == this.size
}

func (this *ackqueue) empty() bool {
	return this.count == 0
}

func (this *ackqueue) increment(n int64) int64 {
	return this.index(n + 1)
}
