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

package session

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrKeyNotAvailable error = errors.New("Session: not item found for key.")
)

var (
	_ SessionProvider = (*memProvider)(nil)
	_ Session         = (*memSession)(nil)
)

type memSession struct {
	id     string
	mu     sync.RWMutex
	values map[interface{}]interface{}
}

func init() {
	Register("mem", NewMemProvider())
}

func (this *memSession) Set(key, value interface{}) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.values[key] = value
	return nil
}

func (this *memSession) Get(key interface{}) (interface{}, error) {
	this.mu.RLock()
	defer this.mu.RUnlock()

	v, ok := this.values[key]
	if !ok {
		return nil, ErrKeyNotAvailable
	}

	return v, nil
}

func (this *memSession) Del(key interface{}) {
	this.mu.Lock()
	defer this.mu.Unlock()
	delete(this.values, key)
}

func (this *memSession) ID() string {
	return this.id
}

// **** Below are memProvider methods ***** //

type memProvider struct {
	st map[string]Session
	mu sync.RWMutex
}

func NewMemProvider() *memProvider {
	return &memProvider{
		st: make(map[string]Session),
	}
}

func (this *memProvider) New(id string) (Session, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	sess := &memSession{id: id, values: make(map[interface{}]interface{})}
	this.st[id] = sess
	return sess, nil
}

func (this *memProvider) Get(id string) (Session, error) {
	this.mu.RLock()
	defer this.mu.RUnlock()

	sess, ok := this.st[id]
	if !ok {
		return nil, fmt.Errorf("store/Get: No session found for key %s", id)
	}

	return sess, nil
}

func (this *memProvider) Del(id string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	delete(this.st, id)
}

func (this *memProvider) Save(id string) error {
	return nil
}

func (this *memProvider) Count() int {
	return len(this.st)
}
