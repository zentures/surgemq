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
	"io"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/dataence/glog"
)

type server struct {
	service
}

func ListenAndServe(uri string) error {
	u, err := url.Parse(uri)
	if err != nil {
		return err
	}

	ln, err := net.Listen(u.Scheme, u.Host)
	if err != nil {
		return err
	}
	defer ln.Close()

	glog.Infof("server/ListenAndServe: server is ready...")

	for {
		conn, err := ln.Accept()
		if err != nil {
			glog.Errorf("server/ListenAndServe: Error accpeting connection: %v", err)
			return err
		}

		go handleConnection(conn)
	}

	return nil
}

// HandleConnection is for the broker to handle an incoming connection from a client
func handleConnection(conn io.Closer) (*server, error) {
	if conn == nil {
		return nil, fmt.Errorf("Connection is nil")
	}

	tcpConn, ok := conn.(net.Conn)
	if !ok {
		glog.Errorf("Invalid TCP Connection: %v", ErrInvalidConnectionType)
		return nil, ErrInvalidConnectionType
	}

	svc := &server{
		service{
			id:     atomic.AddUint64(&gsvcid, 1),
			conn:   conn,
			client: false,
		},
	}

	tcpConn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(options.ConnectTimeout)))

	if err := svc.initSendRecv(); err != nil {
		glog.Errorf("(%d) Error initiating sender and receiver: %v", svc.id, err)
		svc.Disconnect()
		return nil, err
	}

	if err := svc.connectClient(); err != nil {
		glog.Errorf("(%d) Error connecting client: %v", svc.id, err)
		return nil, err
	}

	tcpConn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(options.KeepAlive)))

	if err := svc.initProcessor(); err != nil {
		glog.Errorf("(%d) Error initiating processor: %v", svc.id, err)
		svc.Disconnect()
		return nil, err
	}

	glog.Infof("(%d) server/handleConnection: Connection established with client %s.", svc.id, svc.cid)

	return svc, nil
}
