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

package message

// ConnackCode is the type representing the return code in the CONNACK message,
// returned after the initial CONNECT message
type ConnackCode byte

const (
	// Connection accepted
	ConnectionAccepted ConnackCode = iota

	// The Server does not support the level of the MQTT protocol requested by the Client
	ErrInvalidProtocolVersion

	// The Client identifier is correct UTF-8 but not allowed by the server
	ErrIdentifierRejected

	// The Network Connection has been made but the MQTT service is unavailable
	ErrServerUnavailable

	// The data in the user name or password is malformed
	ErrBadUsernameOrPassword

	// The Client is not authorized to connect
	ErrNotAuthorized
)

// Value returns the value of the ConnackCode, which is just the byte representation
func (this ConnackCode) Value() byte {
	return byte(this)
}

// Desc returns the description of the ConnackCode
func (this ConnackCode) Desc() string {
	switch this {
	case 0:
		return "Connection accepted"
	case 1:
		return "The Server does not support the level of the MQTT protocol requested by the Client"
	case 2:
		return "The Client identifier is correct UTF-8 but not allowed by the server"
	case 3:
		return "The Network Connection has been made but the MQTT service is unavailable"
	case 4:
		return "The data in the user name or password is malformed"
	case 5:
		return "The Client is not authorized to connect"
	}

	return ""
}

// Valid checks to see if the ConnackCode is valid. Currently valid codes are <= 5
func (this ConnackCode) Valid() bool {
	return this <= 5
}

// Error returns the corresonding error string for the ConnackCode
func (this ConnackCode) Error() string {
	switch this {
	case 0:
		return "Connection accepted"
	case 1:
		return "Connection Refused, unacceptable protocol version"
	case 2:
		return "Connection Refused, identifier rejected"
	case 3:
		return "Connection Refused, Server unavailable"
	case 4:
		return "Connection Refused, bad user name or password"
	case 5:
		return "Connection Refused, not authorized"
	}

	return "Unknown error"
}
