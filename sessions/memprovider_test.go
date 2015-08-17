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

package sessions

/*
func TestMemStore(t *testing.T) {
	st := NewMemStore()

	for i := 0; i < 10; i++ {
		st.New(fmt.Sprintf("%d", i))
	}

	require.Equal(t, 10, st.Len(), "Incorrect length.")

	for i := 0; i < 10; i++ {
		sess, err := st.Get(fmt.Sprintf("%d", i))
		require.NoError(t, err, "Unable to Get() session #%d", i)

		require.Equal(t, fmt.Sprintf("%d", i), sess.ClientId, "Incorrect ID")
	}

	for i := 0; i < 5; i++ {
		st.Del(fmt.Sprintf("%d", i))
	}

	require.Equal(t, 5, st.Len(), "Incorrect length.")
}
*/
