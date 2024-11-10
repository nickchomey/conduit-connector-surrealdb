package surrealdb

import (
	"context"
	"testing"

	"github.com/matryer/is"
	surrealdb "github.com/nickchomey/conduit-connector-surrealdb/source"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := surrealdb.NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
