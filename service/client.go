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
	"fmt"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/surge/surgemq/message"
	"github.com/surge/surgemq/sessions"
	"github.com/surge/surgemq/topics"
)

const (
	minKeepAlive = 30
)

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

// Connect is for MQTT clients to open a connection to a remote server
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

func (this *Client) Publish(msg *message.PublishMessage, onComplete OnCompleteFunc) error {
	return this.svc.publish(msg, onComplete)
}

func (this *Client) Subscribe(msg *message.SubscribeMessage, onComplete OnCompleteFunc, onPublish OnPublishFunc) error {
	return this.svc.subscribe(msg, onComplete, onPublish)
}

func (this *Client) Unsubscribe(msg *message.UnsubscribeMessage, onComplete OnCompleteFunc) error {
	return this.svc.unsubscribe(msg, onComplete)
}

func (this *Client) Ping(onComplete OnCompleteFunc) error {
	return this.svc.ping(onComplete)
}

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
