package surrealdb

import (
	"context"
	"testing"

	"github.com/matryer/is"
	surrealdb "github.com/nickchomey/conduit-connector-surrealdb/destination"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := surrealdb.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
