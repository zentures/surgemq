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

package sessions

import "time"

type Session struct {
	ClientId    string
	Username    string
	WillRetain  bool
	WillQos     byte
	WillTopic   string
	WillMessage string
	Version     byte
	KeepAlive   time.Duration
	Topics      []string
	Qos         []byte
}

func (this *Session) AddTopic(topic string, qos byte) {
	// Update subscription if already exist
	for i, t := range this.Topics {
		if topic == t {
			this.Qos[i] = qos
			return
		}
	}

	// Otherwise add it
	this.Topics = append(this.Topics, topic)
	this.Qos = append(this.Qos, qos)
}

func (this *Session) RemoveTopic(topic string) {
	// Delete subscription if already exist
	for i, t := range this.Topics {
		if topic == t {
			this.Topics = append(this.Topics[:i], this.Topics[i+1:]...)
			this.Qos = append(this.Qos[:i], this.Qos[i+1:]...)
			return
		}
	}
}
