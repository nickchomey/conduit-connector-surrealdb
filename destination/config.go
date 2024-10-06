package destination

// Config contains shared config parameters, common to the source and
// destination. If you don't need shared parameters you can entirely remove this
// file.
import (
	"github.com/nickchomey/conduit-connector-surrealdb/common"
)

//go:generate paramgen -output=paramgen.go Config
type Config struct {
	common.Config
	// We will always set an "id" field. If the incoming primary key is not "id", then "id" will get its value. DeleteOldKey is a flag to delete the old key and value from payload. Set to false if you want to keep the old key and value in the payload.
	DeleteOldKey bool `json:"delete_old_key" default:"false"`
}
