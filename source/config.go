package source

// Config contains shared config parameters, common to the source and
// destination. If you don't need shared parameters you can entirely remove this
// file.
import (
	"github.com/nickchomey/conduit-connector-surrealdb/common"
)

//go:generate paramgen -output=paramgen.go Config
type Config struct {
	common.Config
}
