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

package auth

import (
	"testing"

	"github.com/dataence/assert"
)

func TestMockSuccessAuthenticator(t *testing.T) {
	assert.NoError(t, true, mockSuccessAuthenticator.Authenticate("", ""))

	assert.NoError(t, true, providers["mockSuccess"].Authenticate("", ""))

	mgr, err := NewManager("mockSuccess")
	assert.NoError(t, true, err)
	assert.NoError(t, true, mgr.Authenticate("", ""))
}

func TestMockFailureAuthenticator(t *testing.T) {
	assert.Error(t, true, mockFailureAuthenticator.Authenticate("", ""))

	assert.Error(t, true, providers["mockFailure"].Authenticate("", ""))

	mgr, err := NewManager("mockFailure")
	assert.NoError(t, true, err)
	assert.Error(t, true, mgr.Authenticate("", ""))
}
