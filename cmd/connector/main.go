package main

import (
	surrealdb "github.com/nickchomey/conduit-connector-surrealdb"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(surrealdb.Connector)
}
