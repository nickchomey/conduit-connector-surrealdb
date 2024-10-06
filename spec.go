package surrealdb

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// version is set during the build process with ldflags (see Makefile).
// Default version matches default from runtime/debug.
var version = "(devel)"

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:        "surrealdb",
		Summary:     "A conduit connector for SurrealDB",
		Description: "Destination connector for SurrealDB that imports data from Conduit and can create, update, and delete records as well as graph relationships defined in the schema registry",
		Version:     version,
		Author:      "Nick Chomey",
	}
}
