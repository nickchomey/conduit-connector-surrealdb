package surrealdb_test

import (
	"context"
	"testing"

	surrealdb "github.com/nickchomey/conduit-connector-surrealdb"
	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := surrealdb.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
