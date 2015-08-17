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
	"fmt"
	"sync"

	"github.com/surgemq/message"
)

const (
	// Queue size for the ack queue
	defaultQueueSize = 16
)

type Session struct {
	// Ack queue for outgoing PUBLISH QoS 1 messages
	Pub1ack *Ackqueue

	// Ack queue for incoming PUBLISH QoS 2 messages
	Pub2in *Ackqueue

	// Ack queue for outgoing PUBLISH QoS 2 messages
	Pub2out *Ackqueue

	// Ack queue for outgoing SUBSCRIBE messages
	Suback *Ackqueue

	// Ack queue for outgoing UNSUBSCRIBE messages
	Unsuback *Ackqueue

	// Ack queue for outgoing PINGREQ messages
	Pingack *Ackqueue

	// cmsg is the CONNECT message
	Cmsg *message.ConnectMessage

	// Will message to publish if connect is closed unexpectedly
	Will *message.PublishMessage

	// Retained publish message
	Retained *message.PublishMessage

	// cbuf is the CONNECT message buffer, this is for storing all the will stuff
	cbuf []byte

	// rbuf is the retained PUBLISH message buffer
	rbuf []byte

	// topics stores all the topis for this session/client
	topics map[string]byte

	// Initialized?
	initted bool

	// Serialize access to this session
	mu sync.Mutex

	id string
}

func (this *Session) Init(msg *message.ConnectMessage) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.initted {
		return fmt.Errorf("Session already initialized")
	}

	this.cbuf = make([]byte, msg.Len())
	this.Cmsg = message.NewConnectMessage()

	if _, err := msg.Encode(this.cbuf); err != nil {
		return err
	}

	if _, err := this.Cmsg.Decode(this.cbuf); err != nil {
		return err
	}

	if this.Cmsg.WillFlag() {
		this.Will = message.NewPublishMessage()
		this.Will.SetQoS(this.Cmsg.WillQos())
		this.Will.SetTopic(this.Cmsg.WillTopic())
		this.Will.SetPayload(this.Cmsg.WillMessage())
		this.Will.SetRetain(this.Cmsg.WillRetain())
	}

	this.topics = make(map[string]byte, 1)

	this.id = string(msg.ClientId())

	this.Pub1ack = newAckqueue(defaultQueueSize)
	this.Pub2in = newAckqueue(defaultQueueSize)
	this.Pub2out = newAckqueue(defaultQueueSize)
	this.Suback = newAckqueue(defaultQueueSize)
	this.Unsuback = newAckqueue(defaultQueueSize)
	this.Pingack = newAckqueue(defaultQueueSize)

	this.initted = true

	return nil
}

func (this *Session) Update(msg *message.ConnectMessage) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.cbuf = make([]byte, msg.Len())
	this.Cmsg = message.NewConnectMessage()

	if _, err := msg.Encode(this.cbuf); err != nil {
		return err
	}

	if _, err := this.Cmsg.Decode(this.cbuf); err != nil {
		return err
	}

	return nil
}

func (this *Session) RetainMessage(msg *message.PublishMessage) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.rbuf = make([]byte, msg.Len())
	this.Retained = message.NewPublishMessage()

	if _, err := msg.Encode(this.rbuf); err != nil {
		return err
	}

	if _, err := this.Retained.Decode(this.rbuf); err != nil {
		return err
	}

	return nil
}

func (this *Session) AddTopic(topic string, qos byte) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if !this.initted {
		return fmt.Errorf("Session not yet initialized")
	}

	this.topics[topic] = qos

	return nil
}

func (this *Session) RemoveTopic(topic string) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if !this.initted {
		return fmt.Errorf("Session not yet initialized")
	}

	delete(this.topics, topic)

	return nil
}

func (this *Session) Topics() ([]string, []byte, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if !this.initted {
		return nil, nil, fmt.Errorf("Session not yet initialized")
	}

	var (
		topics []string
		qoss   []byte
	)

	for k, v := range this.topics {
		topics = append(topics, k)
		qoss = append(qoss, v)
	}

	return topics, qoss, nil
}

func (this *Session) ID() string {
	return string(this.Cmsg.ClientId())
}
