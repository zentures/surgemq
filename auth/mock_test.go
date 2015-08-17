// Copyright (c) 2014 The SurgeMQ Authors. All rights reserved.
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

	"github.com/stretchr/testify/require"
)

func TestMockSuccessAuthenticator(t *testing.T) {
	require.NoError(t, mockSuccessAuthenticator.Authenticate("", ""))

	require.NoError(t, providers["mockSuccess"].Authenticate("", ""))

	mgr, err := NewManager("mockSuccess")
	require.NoError(t, err)
	require.NoError(t, mgr.Authenticate("", ""))
}

func TestMockFailureAuthenticator(t *testing.T) {
	require.Error(t, mockFailureAuthenticator.Authenticate("", ""))

	require.Error(t, providers["mockFailure"].Authenticate("", ""))

	mgr, err := NewManager("mockFailure")
	require.NoError(t, err)
	require.Error(t, mgr.Authenticate("", ""))
}
