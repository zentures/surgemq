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
)

type Client struct {
	service
}

// Connect is for MQTT clients to open a connection to a remote server
func Connect(uri string, msg *message.ConnectMessage) (*Client, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "tcp" {
		return nil, ErrInvalidConnectionType
	}

	conn, err := net.Dial(u.Scheme, u.Host)
	if err != nil {
		return nil, err
	}

	if msg == nil {
		return nil, fmt.Errorf("msg is nil")
	}

	svc := &Client{
		service{
			id:     atomic.AddUint64(&gsvcid, 1),
			conn:   conn,
			client: true,
		},
	}

	conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(options.ConnectTimeout)))

	svc.cid = string(msg.ClientId())

	if err := svc.initSendRecv(); err != nil {
		svc.Disconnect()
		return nil, err
	}

	if err := svc.connectToServer(msg); err != nil {
		svc.Disconnect()
		return nil, err
	}

	conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(options.KeepAlive)))

	if err := svc.initProcessor(); err != nil {
		svc.Disconnect()
		return nil, err
	}

	return svc, nil
}
