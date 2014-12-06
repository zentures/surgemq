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

package main

import (
	"github.com/surge/surgemq/auth"
	"github.com/surge/surgemq/service"
	"github.com/surge/surgemq/session"
	"github.com/surge/surgemq/topics"
)

func main() {
	ctx := service.Context{
		KeepAlive:      service.DefaultKeepAlive,
		ConnectTimeout: service.DefaultConnectTimeout,
		AckTimeout:     service.DefaultAckTimeout,
		TimeoutRetries: service.DefaultTimeoutRetries,
		Auth:           auth.MockSuccessAuthenticator,
		Topics:         topics.NewMemTopics(),
		Store:          session.NewMemStore(),
	}

	service.ListenAndServe(ctx, "tcp://127.0.0.1:1883")
}
