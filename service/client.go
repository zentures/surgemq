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

package service

import (
	"fmt"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/surgemq/message"
	"github.com/surgemq/surgemq/sessions"
	"github.com/surgemq/surgemq/topics"
)

const (
	minKeepAlive = 30
)

// Client is a library implementation of the MQTT client that, as best it can, complies
// with the MQTT 3.1 and 3.1.1 specs.
type Client struct {
	// The number of seconds to keep the connection live if there's no data.
	// If not set then default to 5 mins.
	KeepAlive int

	// The number of seconds to wait for the CONNACK message before disconnecting.
	// If not set then default to 2 seconds.
	ConnectTimeout int

	// The number of seconds to wait for any ACK messages before failing.
	// If not set then default to 20 seconds.
	AckTimeout int

	// The number of times to retry sending a packet if ACK is not received.
	// If no set then default to 3 retries.
	TimeoutRetries int

	svc *service
}

// Connect is for MQTT clients to open a connection to a remote server. It needs to
// know the URI, e.g., "tcp://127.0.0.1:1883", so it knows where to connect to. It also
// needs to be supplied with the MQTT CONNECT message.
func (this *Client) Connect(uri string, msg *message.ConnectMessage) (err error) {
	this.checkConfiguration()

	if msg == nil {
		return fmt.Errorf("msg is nil")
	}

	u, err := url.Parse(uri)
	if err != nil {
		return err
	}

	if u.Scheme != "tcp" {
		return ErrInvalidConnectionType
	}

	conn, err := net.Dial(u.Scheme, u.Host)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	if msg.KeepAlive() < minKeepAlive {
		msg.SetKeepAlive(minKeepAlive)
	}

	if err = writeMessage(conn, msg); err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(this.ConnectTimeout)))

	resp, err := getConnackMessage(conn)
	if err != nil {
		return err
	}

	if resp.ReturnCode() != message.ConnectionAccepted {
		return resp.ReturnCode()
	}

	this.svc = &service{
		id:     atomic.AddUint64(&gsvcid, 1),
		client: true,
		conn:   conn,

		keepAlive:      int(msg.KeepAlive()),
		connectTimeout: this.ConnectTimeout,
		ackTimeout:     this.AckTimeout,
		timeoutRetries: this.TimeoutRetries,
	}

	err = this.getSession(this.svc, msg, resp)
	if err != nil {
		return err
	}

	p := topics.NewMemProvider()
	topics.Register(this.svc.sess.ID(), p)

	this.svc.topicsMgr, err = topics.NewManager(this.svc.sess.ID())
	if err != nil {
		return err
	}

	if err := this.svc.start(); err != nil {
		this.svc.stop()
		return err
	}

	this.svc.inStat.increment(int64(msg.Len()))
	this.svc.outStat.increment(int64(resp.Len()))

	return nil
}

// Publish sends a single MQTT PUBLISH message to the server. On completion, the
// supplied OnCompleteFunc is called. For QOS 0 messages, onComplete is called
// immediately after the message is sent to the outgoing buffer. For QOS 1 messages,
// onComplete is called when PUBACK is received. For QOS 2 messages, onComplete is
// called after the PUBCOMP message is received.
func (this *Client) Publish(msg *message.PublishMessage, onComplete OnCompleteFunc) error {
	return this.svc.publish(msg, onComplete)
}

// Subscribe sends a single SUBSCRIBE message to the server. The SUBSCRIBE message
// can contain multiple topics that the client wants to subscribe to. On completion,
// which is when the client receives a SUBACK messsage back from the server, the
// supplied onComplete funciton is called.
//
// When messages are sent to the client from the server that matches the topics the
// client subscribed to, the onPublish function is called to handle those messages.
// So in effect, the client can supply different onPublish functions for different
// topics.
func (this *Client) Subscribe(msg *message.SubscribeMessage, onComplete OnCompleteFunc, onPublish OnPublishFunc) error {
	return this.svc.subscribe(msg, onComplete, onPublish)
}

// Unsubscribe sends a single UNSUBSCRIBE message to the server. The UNSUBSCRIBE
// message can contain multiple topics that the client wants to unsubscribe. On
// completion, which is when the client receives a UNSUBACK message from the server,
// the supplied onComplete function is called. The client will no longer handle
// messages from the server for those unsubscribed topics.
func (this *Client) Unsubscribe(msg *message.UnsubscribeMessage, onComplete OnCompleteFunc) error {
	return this.svc.unsubscribe(msg, onComplete)
}

// Ping sends a single PINGREQ message to the server. PINGREQ/PINGRESP messages are
// mainly used by the client to keep a heartbeat to the server so the connection won't
// be dropped.
func (this *Client) Ping(onComplete OnCompleteFunc) error {
	return this.svc.ping(onComplete)
}

// Disconnect sends a single DISCONNECT message to the server. The client immediately
// terminates after the sending of the DISCONNECT message.
func (this *Client) Disconnect() {
	//msg := message.NewDisconnectMessage()
	this.svc.stop()
}

func (this *Client) getSession(svc *service, req *message.ConnectMessage, resp *message.ConnackMessage) error {
	//id := string(req.ClientId())
	svc.sess = &sessions.Session{}
	return svc.sess.Init(req)
}

func (this *Client) checkConfiguration() {
	if this.KeepAlive == 0 {
		this.KeepAlive = DefaultKeepAlive
	}

	if this.ConnectTimeout == 0 {
		this.ConnectTimeout = DefaultConnectTimeout
	}

	if this.AckTimeout == 0 {
		this.AckTimeout = DefaultAckTimeout
	}

	if this.TimeoutRetries == 0 {
		this.TimeoutRetries = DefaultTimeoutRetries
	}
}
