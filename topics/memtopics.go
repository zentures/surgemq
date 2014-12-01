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

package topics

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/dataence/glog"
	"github.com/surge/surgemq/message"
)

var (
	MaxQosAllowed byte = message.QosExactlyOnce
)

type MemTopics struct {
	mu   sync.RWMutex
	root *node
}

var _ Topics = (*MemTopics)(nil)

func NewMemTopics() *MemTopics {
	return &MemTopics{
		root: newNode(),
	}
}

func (this *MemTopics) Subscribe(topic []byte, qos byte, sub interface{}) (byte, error) {
	if !message.ValidQos(qos) {
		//glog.Errorf("Invalid QoS %d", qos)
		return message.QosFailure, fmt.Errorf("Invalid QoS %d", qos)
	}

	if sub == nil {
		return message.QosFailure, fmt.Errorf("Subscriber cannot be nil")
	}

	this.mu.Lock()
	defer this.mu.Unlock()

	if qos > MaxQosAllowed {
		qos = MaxQosAllowed
	}

	if err := this.root.insert(topic, qos, sub); err != nil {
		//glog.Errorf("%v", err)
		return message.QosFailure, err
	}

	return qos, nil
}

func (this *MemTopics) Unsubscribe(topic []byte, sub interface{}) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	return this.root.remove(topic, sub)
}

// Returned values will be invalidated by the next Subscribers call
func (this *MemTopics) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	if !message.ValidQos(qos) {
		return fmt.Errorf("Invalid QoS %d", qos)
	}

	this.mu.RLock()
	defer this.mu.RUnlock()

	*subs = (*subs)[0:0]
	*qoss = (*qoss)[0:0]

	glog.Debugf("len subs = %d", len(*subs))

	return this.root.match(topic, qos, subs, qoss)
}

type node struct {
	// If this is the end of the topic string, then add subscribers here
	subs []interface{}
	qos  []byte

	// Otherwise add the next topic level here
	nodes map[string]*node
}

func newNode() *node {
	return &node{
		nodes: make(map[string]*node, 4),
	}
}

func (this *node) insert(topic []byte, qos byte, sub interface{}) error {
	// If there's no more topic levels, that means we are at the matching node
	// to insert the subscriber. So let's see if there's such subscriber,
	// if so, update it. Otherwise insert it.
	if len(topic) == 0 {
		// Let's see if the subscriber is already on the list. If yes, update
		// QoS and then return.
		for i := range this.subs {
			if equal(this.subs[i], sub) {
				this.qos[i] = qos
				return nil
			}
		}

		// Otherwise add.
		this.subs = append(this.subs, sub)
		this.qos = append(this.qos, qos)

		return nil
	}

	// Not the last level, so let's find or create the next level node, and
	// recursively call it's insert().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Add node if it doesn't already exist
	n, ok := this.nodes[level]
	if !ok {
		n = newNode()
		this.nodes[level] = n
	}

	return n.insert(rem, qos, sub)
}

// This remove implementation ignores the QoS, as long as the subscriber
// matches then it's removed
func (this *node) remove(topic []byte, sub interface{}) error {
	// If the topic is empty, it means we are at the final matching node. If so,
	// let's find the matching subscribers and remove them.
	if len(topic) == 0 {
		// If subscriber == nil, then it's signal to remove ALL subscribers
		if sub == nil {
			this.subs = this.subs[0:0]
			this.qos = this.qos[0:0]
			return nil
		}

		// If we find the subscriber then remove it from the list. Technically
		// we just overwrite the slot by shifting all other items up by one.
		for i := range this.subs {
			if equal(this.subs[i], sub) {
				this.subs = append(this.subs[:i], this.subs[i+1:]...)
				this.qos = append(this.qos[:i], this.qos[i+1:]...)
				return nil
			}
		}

		return fmt.Errorf("No topic found for subscriber")
	}

	// Not the last level, so let's find the next level node, and recursively
	// call it's remove().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Find the node that matches the topic level
	n, ok := this.nodes[level]
	if !ok {
		return fmt.Errorf("No topic found")
	}

	// Remove the subscriber from the next level node
	if err := n.remove(rem, sub); err != nil {
		return err
	}

	// If there are no more subscribers and nodes to the next level we just visited
	// let's remove it
	if len(n.subs) == 0 && len(n.nodes) == 0 {
		delete(this.nodes, level)
	}

	return nil
}

// match returns all the subscribers that are subscribed to the topic
// For each of the level names, it's a match
// - if there are subscribers to '#', then all the subscribers are added to result set
func (this *node) match(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	// If the topic is empty, it means we are at the final matching node. If so,
	// let's find the subscribers that match the qos and append them to the list.
	if len(topic) == 0 {
		this.matchQos(qos, subs, qoss)
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	for k, n := range this.nodes {
		// If the key is "#", then these subscribers are added to the result set
		if k == "#" {
			n.matchQos(qos, subs, qoss)
		} else if k == "+" || k == level {
			if err := n.match(rem, qos, subs, qoss); err != nil {
				return err
			}
		}
	}

	return nil
}

const (
	stateCHR byte = iota // Regular character
	stateMWC             // Multi-level wildcard
	stateSWC             // Single-level wildcard
	stateSEP             // Topic level separator
	stateSYS             // System level topic ($)
)

// Returns topic level, remaining topic levels and any errors
func nextTopicLevel(topic []byte) ([]byte, []byte, error) {
	s := stateCHR

	for i, c := range topic {
		switch c {
		case '/':
			if s == stateMWC {
				return nil, nil, fmt.Errorf("Invalid topic: multi-level wildcard found and it's not at the last level")
			}

			if i == 0 {
				return []byte("+"), topic[i+1:], nil
			} else {
				return topic[:i], topic[i+1:], nil
			}

		case '#':
			if i != 0 {
				return nil, nil, fmt.Errorf("Invalid topic: wildcard characters must occupy entire level")
			}

			s = stateMWC

		case '+':
			if i != 0 {
				return nil, nil, fmt.Errorf("Invalid topic: wildcard characters must occupy entire level")
			}

			s = stateSWC

		case '$':
			if i != 0 {
				return nil, nil, fmt.Errorf("Invalid topic: wildcard characters must occupy entire level")
			}

			s = stateSYS

		default:
			if s == stateMWC || s == stateSWC {
				return nil, nil, fmt.Errorf("Invalid topic: wildcard characters must occupy entire level")
			}

			s = stateCHR
		}
	}

	// If we got here that means we didn't hit the separator along the way, so the
	// topic is either empty, or does not contain a separator. Either way, we return
	// the full topic
	return topic, nil, nil
}

// The QoS of the payload messages sent in response to a subscription must be the
// minimum of the QoS of the originally published message (in this case, it's the
// qos parameter) and the maximum QoS granted by the server (in this case, it's
// the QoS in the topic tree).
//
// It's also possible that even if the topic matches, the subscriber is not included
// due to the QoS granted is lower than the published message QoS. For example,
// if the client is granted only QoS 0, and the publish message is QoS 1, then this
// client is not to be send the published message.
func (this *node) matchQos(qos byte, subs *[]interface{}, qoss *[]byte) {
	for i, sub := range this.subs {
		// If the published QoS is higher than the subscriber QoS, then we skip the
		// subscriber. Otherwise, add to the list.
		if qos <= this.qos[i] {
			*subs = append(*subs, sub)
			*qoss = append(*qoss, qos)
		}
	}

	glog.Debugf("Added %d subscribers", len(*subs))
}

func equal(k1, k2 interface{}) bool {
	if k1 == k2 {
		return true
	}

	if reflect.TypeOf(k1) != reflect.TypeOf(k2) {
		return false
	}

	switch k1 := k1.(type) {
	case string:
		return k1 == k2.(string)

	case int64:
		return k1 == k2.(int64)

	case int32:
		return k1 == k2.(int32)

	case int16:
		return k1 == k2.(int16)

	case int8:
		return k1 == k2.(int8)

	case int:
		return k1 == k2.(int)

	case float32:
		return k1 == k2.(float32)

	case float64:
		return k1 == k2.(float64)

	case uint:
		return k1 == k2.(uint)

	case uint8:
		return k1 == k2.(uint8)

	case uint16:
		return k1 == k2.(uint16)

	case uint32:
		return k1 == k2.(uint32)

	case uint64:
		return k1 == k2.(uint64)

	case uintptr:
		return k1 == k2.(uintptr)
	}

	return false
}
