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
	"time"

	"github.com/surge/surgemq/auth"
	"github.com/surge/surgemq/sessions"
	"github.com/surge/surgemq/topics"
)

const (
	DefaultKeepAlive      = 300000000000
	DefaultConnectTimeout = 2000000000
	DefaultAckTimeout     = 20000000000
	DefaultTimeoutRetries = 3
	DefaultSessionStore   = "mem"
	DefaultAuthMethod     = "mock"
)

type Context struct {
	// The number of seconds to keep the connection live if there's no data, default 5 mins
	KeepAlive time.Duration

	// The number of seconds to wait for the CONNECT message before disconnecting
	ConnectTimeout time.Duration

	// The number of seconds to wait for any ACK messages before failing
	AckTimeout time.Duration

	// The number of times to retry sending a packet if ACK is not received
	TimeoutRetries int

	// auth is the authenticator used to check username and password sent in the CONNECT message
	Auth auth.Authenticator

	// sessions is the session store that keeps all the Session objects. This is the store to
	// check if CleanSession is set to 0 in the CONNECT message
	Store sessions.Store

	// Topic subscriptions
	Topics topics.Topics
}

func NewContext() Context {
	return Context{
		KeepAlive:      DefaultKeepAlive,
		ConnectTimeout: DefaultConnectTimeout,
		AckTimeout:     DefaultAckTimeout,
		TimeoutRetries: DefaultTimeoutRetries,
	}
}

func (this Context) valid() bool {
	if this.KeepAlive > 0 && this.ConnectTimeout > 0 && this.AckTimeout > 0 && this.TimeoutRetries > 0 &&
		this.Auth != nil && this.Store != nil && this.Topics != nil {
		return true
	}

	return false
}
