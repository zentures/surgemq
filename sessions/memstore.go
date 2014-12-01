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

import (
	"fmt"
	"sync"
)

type MemStore struct {
	st map[string]*Session
	mu sync.RWMutex
}

var _ Store = (*MemStore)(nil)

func NewMemStore() *MemStore {
	return &MemStore{
		st: make(map[string]*Session),
	}
}

func (this *MemStore) New(id string) (*Session, error) {
	sess := &Session{ClientId: id}
	this.Put(id, sess)
	return sess, nil
}

func (this *MemStore) Get(id string) (*Session, error) {
	this.mu.RLock()
	defer this.mu.RUnlock()

	sess, ok := this.st[id]
	if !ok {
		return nil, fmt.Errorf("store/Get: No session found for key %s", id)
	}

	return sess, nil
}

func (this *MemStore) Put(id string, sess *Session) {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.st[id] = sess
}

func (this *MemStore) Del(id string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	delete(this.st, id)
}

func (this *MemStore) Len() int {
	return len(this.st)
}
