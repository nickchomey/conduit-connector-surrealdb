package surrealdb

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nickchomey/conduit-connector-surrealdb/destination"
	"github.com/nickchomey/conduit-connector-surrealdb/source"
)

// Connector combines all constructors for each plugin in one struct.
var Connector = sdk.Connector{
	NewSpecification: Specification,
	NewSource:        source.NewSource,
	NewDestination:   destination.NewDestination,
}
