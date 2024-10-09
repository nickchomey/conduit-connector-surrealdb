// Copyright © 2022 Meroxa, Inc.
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

package common

// Config contains configurable values
// shared between source and destination SurrealDB connector.

type Config struct {
	// URL is the connection string for the SurrealDB server.
	URL string `json:"url" validate:"required"`
	// Username is the username for the SurrealDB server.
	Username string `json:"username" validate:"required"`
	// Password is the password for the SurrealDB server.
	Password string `json:"password" validate:"required"`
	// Namespace is the namespace for the SurrealDB server.
	Namespace string `json:"namespace" validate:"required"`
	// Database is the database name for the SurrealDB server.
	Database string `json:"database" validate:"required"`
	// Scope is the scope for the SurrealDB server.
	Scope string `json:"scope" validate:"required"`
}
