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

import "flag"

const (
	defaultKeepAlive       = 300
	defaultConnectTimeout  = 2
	defaultAckTimeout      = 20
	defaultTimeoutRetries  = 3
	defaultSessionProvider = "mem"
	defaultAuthenticator   = "mockSuccess"
)

type Options struct {
	// The number of seconds to keep the connection live if there's no data, default 5 mins
	KeepAlive int

	// The number of seconds to wait for the CONNECT message before disconnecting
	ConnectTimeout int

	// The number of seconds to wait for any ACK messages before failing
	AckTimeout int

	// The number of times to retry sending a packet if ACK is not received
	TimeoutRetries int

	// auth is the authenticator used to check username and password sent in the CONNECT message
	Authenticator string

	// sessions is the session store that keeps all the Session objects. This is the store to
	// check if CleanSession is set to 0 in the CONNECT message
	SessionProvider string
}

var (
	options Options
)

func init() {
	flag.IntVar(&options.KeepAlive, "keepalive", defaultKeepAlive, "Keepalive (sec)")
	flag.IntVar(&options.ConnectTimeout, "connect", defaultConnectTimeout, "Connect Timeout (sec)")
	flag.IntVar(&options.AckTimeout, "ack", defaultAckTimeout, "Ack Timeout (sec)")
	flag.IntVar(&options.TimeoutRetries, "retries", defaultTimeoutRetries, "Timeout Retries")
	flag.StringVar(&options.Authenticator, "auth", defaultAuthenticator, "Authenticator Type")
	flag.StringVar(&options.SessionProvider, "sessions", defaultSessionProvider, "Session Provide Type")
	//flag.Parse()
}
