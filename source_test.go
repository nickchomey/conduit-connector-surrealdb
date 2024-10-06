package surrealdb_test

import (
	"context"
	"testing"

	surrealdb "github.com/nickchomey/conduit-connector-surrealdb"
	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := surrealdb.NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
