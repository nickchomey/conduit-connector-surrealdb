package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"text/template"
	"time"

	//"strings"

	// "encoding/json"
	"fmt"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
	"gopkg.in/yaml.v3"
)

type Destination struct {
	sdk.UnimplementedDestination

	config Config
	db     *surrealdb.DB
	token  string
}

type RelationEventConfig struct {
	Name    string `yaml:"name"`
	Trigger struct {
		Table    string `yaml:"table"`
		InField  string `yaml:"inField"`
		OutField string `yaml:"outField"`
	} `yaml:"trigger"`
	InTable  string `yaml:"inTable"`
	OutTable string `yaml:"outTable"`
}

type RelationSchema struct {
	Relations []RelationEventConfig `yaml:"relations"`
}

func NewDestination() sdk.Destination {
	// Create Destination and wrap it in the default middleware.
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() config.Parameters {
	// Parameters is a map of named Parameters that describe how to configure
	// the Destination. Parameters can be generated from DestinationConfig with
	// paramgen.
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg config.Config) error {
	// Configure is the first function to be called in a connector. It provides
	// the connector with the configuration that can be validated and stored.
	// In case the configuration is not valid it should return an error.
	// Testing if your connector can reach the configured data source should be
	// done in Open, not in Configure.
	// The SDK will validate the configuration and populate default values
	// before calling Configure. If you need to do more complex validations you
	// can do them manually here.

	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(ctx, cfg, &d.config, NewDestination().Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	// Open is called after Configure to signal the plugin it can prepare to
	// start writing records. If needed, the plugin should open connections in
	// this function.

	sdk.Logger(ctx).Info().Msg("Connecting to SurrealDB... on " + d.config.URL)

	db, err := surrealdb.New(d.config.URL)
	if err != nil {
		sdk.Logger(ctx).Error().Msg("Failed to create SurrealDB client: " + err.Error())
		return fmt.Errorf("failed to create SurrealDB client: %w", err)
	}

	if err = db.Use(d.config.Namespace, d.config.Database); err != nil {
		sdk.Logger(ctx).Error().Msg("Failed to select namespace and database: " + err.Error())
		return fmt.Errorf("failed to select namespace and database: %w", err)
	}

	authData := &surrealdb.Auth{
		Username: d.config.Username,
		Password: d.config.Password,
		// Database:  d.config.Database,
		// Namespace: d.config.Namespace,
		// Scope:     d.config.Scope,
	}

	token, err := db.SignIn(authData)
	if err != nil {
		sdk.Logger(ctx).Error().Msg("Failed to sign in to SurrealDB: " + err.Error())
		return fmt.Errorf("failed to sign in to SurrealDB: %w", err)
	}

	relations, err := loadRelationSchema("relations_schema.yaml")
	if err != nil {
		panic(err)
	}

	for _, config := range relations {
		if err := createRelationEvent(db, config); err != nil {
			fmt.Printf("failed to create relation %s: %v\n", config.Name, err)
		}
	}

	d.token = token
	d.db = db
	return nil
}

func (d *Destination) Write(ctx context.Context, recs []opencdc.Record) (int, error) {
	// Write writes len(r) records from r to the destination right away without
	// caching. It should return the number of records written from r
	// (0 <= n <= len(r)) and any error encountered that caused the write to
	// stop early. Write must return a non-nil error if it returns n < len(r).

	// log number of recs to be written
	sdk.Logger(ctx).Info().Msg(fmt.Sprintf("Number of records to be written: %d", len(recs)))

	startTime := time.Now()

	// Step 1: Group records by table and operation
	groupedPayloads := make(map[string]map[opencdc.Operation][]*opencdc.Data)

	//TODO: use goroutines here perhaps to process all records in parallel. Though this is generally quite fast. It is the actual CRUD on surrealdb that takes much longer.
	for i := range recs {
		//TODO: verify whether it might cause any problems here by using a pointer to the record. Does that affect something upstream if the same record is used in multiple connectors? Otherwise it seems like a better idea, since we could, in theory, have tens of thousands of records coming in at a time (default fetch size is 50000 PER TABLE in mysql connector, and this receives all tables in an interspersed batch)
		rec := &recs[i]
		tableName := rec.Metadata["opencdc.collection"]
		if _, ok := groupedPayloads[tableName]; !ok {
			groupedPayloads[tableName] = make(map[opencdc.Operation][]*opencdc.Data)
		}
		// pass the record to processRec function to be prepared for insertion
		err := d.processPayload(rec)
		if err != nil {
			return 0, fmt.Errorf("failed to process record: %w", err)
		}

		groupedPayloads[tableName][rec.Operation] = append(groupedPayloads[tableName][rec.Operation], &rec.Payload.After)
	}
	checkpointTime := time.Now()
	sdk.Logger(ctx).Info().Msg(fmt.Sprintf("Time taken to group records: %s", checkpointTime.Sub(startTime)))

	//TODO: perhaps goroutines here - one for each table operation group
	// Step 2: Iterate over the grouped records and process them
	for table, tableOperations := range groupedPayloads {
		for operation, opPayloads := range tableOperations {
			var err error
			switch operation {
			case opencdc.OperationSnapshot, opencdc.OperationCreate:
				err = d.insert(ctx, table, opPayloads)
			case opencdc.OperationUpdate:
				err = d.update(ctx, table, opPayloads)
			case opencdc.OperationDelete:
				err = d.delete(ctx, table, opPayloads)
			default:
				return 0, fmt.Errorf("invalid operation %q", operation)
			}
			if err != nil {
				//TODO: evaluate if should cancel the batch if an error occurs
				// Probably better to send to DLQ, which should be defined in the pipeline config. Could be a specifc NATS JS subject
				//log the error
				sdk.Logger(ctx).Error().Msg("Failed to process records: " + err.Error())
				//return 0, err
			}
		}
	}

	duration := time.Since(checkpointTime)
	sdk.Logger(ctx).Info().Msg(fmt.Sprintf("Time taken to process records: %s", duration))

	return len(recs), nil
}

func (d *Destination) Teardown(_ context.Context) error {
	// Teardown signals to the plugin that all records were written and there
	// will be no more calls to any other function. After Teardown returns, the
	// plugin should be ready for a graceful shutdown.
	if d.db != nil {
		if err := d.db.Invalidate(); err != nil {
			return fmt.Errorf("failed to invalidate token: %w", err)
		}
		return d.db.Close()
	}
	return nil
}

func loadRelationSchema(filepath string) ([]RelationEventConfig, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	var schema RelationSchema
	if err := yaml.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("failed to parse schema: %w", err)
	}

	return schema.Relations, nil
}

func createRelationEvent(db *surrealdb.DB, config RelationEventConfig) error {
	/* TODO: need to add something to allow for the conditionals to specified
	- eg. perhaps event = create is default, likewise after.inField != NONE is default
	- but also want to allow for other conditions to be specified
		- probably needed for wp_bp_activity -> component and type fields
		- even more likely for meta tables, where there's many keys and records that will need to be related to a single parent table record?
			- perhaps best to use a schema to move meta table records/fields into the parent record as a nested object?
	*/
	tmpl := template.Must(template.New("event").Parse(`
DEFINE EVENT IF NOT EXISTS {{.InTable}}_{{.Name}}_{{.OutTable}}_relation ON TABLE {{.Trigger.Table}} 
WHEN $event = 'CREATE' 
AND $after.{{.Trigger.InField}} != NONE 
THEN {
    LET $in = IF type::is::record($after.{{.Trigger.InField}}) {
            $after.{{.Trigger.InField}} 
        } ELSE {
            type::thing('{{.InTable}}', $after.{{.Trigger.InField}})
        };
    LET $out = IF type::is::record($after.{{.Trigger.OutField}}) {
            $after.{{.Trigger.OutField}}
        } ELSE {
            type::thing('{{.OutTable}}', $after.{{.Trigger.OutField}})
        };
    
    RELATE $in->{{.Name}}->$out;
};

DEFINE INDEX {{.Name}}_unique_relationship 
ON TABLE {{.Name}}
COLUMNS in, out UNIQUE;`))

	var query bytes.Buffer
	if err := tmpl.Execute(&query, config); err != nil {
		return err
	}

	queryString := strings.TrimSpace(query.String())
	queryString = strings.ReplaceAll(queryString, "\n", " ")
	queryString = strings.ReplaceAll(queryString, "\t", " ")
	queryString = strings.ReplaceAll(queryString, "    ", " ")
	_, err := surrealdb.Query[interface{}](db, queryString, nil)

	//define unique index on relation table for the relation event just created
	// DEFINE INDEX unique_relationships
	// ON TABLE posted
	// COLUMNS in, out UNIQUE;

	return err
}

// Receive record and return pointer to modified payload
func (d *Destination) processPayload(r *opencdc.Record) error {

	//ensure payload is map[string]interface{}
	err := d.structuredDataFormatter(&r.Payload.After)
	if err != nil {
		return fmt.Errorf("failed to get payload: %w", err)
	}

	// get the first key column name, in case there are many primary keys
	keyColumn, err := d.getKeyColumnName(r.Key)
	if err != nil {
		return err
	}

	// Perform a type assertion to access the underlying map
	if afterMap, ok := r.Payload.After.(opencdc.StructuredData); ok {
		if keyColumn != "id" {
			// Set afterMap["id"] to the value of afterMap[keyColumn]
			if value, exists := afterMap[keyColumn]; exists {
				afterMap["id"] = value
				// Optionally delete afterMap[keyColumn]
				if d.config.DeleteOldKey {
					delete(afterMap, keyColumn)
				}
			}
			// Update the Payload.After with the modified map
			r.Payload.After = afterMap
		}
	} else {
		return fmt.Errorf("unexpected type for r.Payload.After: %T", r.Payload.After)
	}

	return err
}

func (d *Destination) insert(ctx context.Context, tableName string, payloads []*opencdc.Data) error {

	// This only works partially. Problem is that bulk insert doesnt support using `ON DUPLICATE KEY UPDATE`, which can be used for single inserts. So if a bulk insert has an existing key, the whole batch will fail.
	// Given that Bulk Upsert doesnt exist yet (https://github.com/surrealdb/surrealdb/pull/4455), perhaps should make this loop through all records and insert one by one for now, so that at least it'll work rather than fail? Or, if we're just doing one by one, should normal Upsert be used instead of Insert?

	if _, err := surrealdb.Insert[interface{}](d.db, models.Table(tableName), payloads); err != nil {
		sdk.Logger(ctx).Error().Msg("Failed to insert record: " + err.Error())
		return fmt.Errorf("failed to insert record: %w", err)
	}

	return nil
}

func (d *Destination) update(ctx context.Context, tableName string, payloads []*opencdc.Data) error {

	//right now Update doesnt support batched transactions, so need to loop the payloads and update one by one. Variable is "tableName" for now, but is really just a single record. It'll be a table when bulk update/upsert is supported
	for _, payload := range payloads {
		//append id to tableName with colon
		if payloadMap, ok := (*payload).(opencdc.StructuredData); ok {
			tableName = tableName + ":" + fmt.Sprintf("%v", payloadMap["id"])
			//remove id from payload as it conflicts with Update/Delete commands
			delete(payloadMap, "id")
			*payload = payloadMap
		} else {
			return fmt.Errorf("unexpected type for payload: %T", *payload)
		}

		//TODO: Update function doesnt actually seem to work. Nor does upsert or merge.
		if _, err := surrealdb.Update[interface{}](d.db, models.Table(tableName), payload); err != nil {
			sdk.Logger(ctx).Error().Msg("Failed to insert record: " + err.Error())
			return fmt.Errorf("failed to insert record: %w", err)
		}

	}
	return nil
}

func (d *Destination) delete(ctx context.Context, tableName string, payloads []*opencdc.Data) error {

	//right now Update doesnt support batched transactions, so need to loop the payloads and delete one by one. Variable is "tableName" for now, but is really just a single record. It'll be a table when bulk delete is supported
	for _, payload := range payloads {
		//append id to tableName with colon
		if payloadMap, ok := (*payload).(opencdc.StructuredData); ok {
			tableName = tableName + ":" + fmt.Sprintf("%v", payloadMap["id"])
			// //remove id from payload
			// delete(payloadMap, "id")
			// *payload = payloadMap
		} else {
			return fmt.Errorf("unexpected type for payload: %T", *payload)
		}

		if _, err := surrealdb.Delete[interface{}](d.db, models.Table(tableName)); err != nil {
			sdk.Logger(ctx).Error().Msg("Failed to insert record: " + err.Error())
			return fmt.Errorf("failed to insert record: %w", err)
		}
	}
	return nil
}

func (d *Destination) getTableName(r opencdc.Record) (string, error) {
	if val, ok := r.Metadata["opencdc.collection"]; !ok || val == "" {
		return "", nil
	}
	return r.Metadata["opencdc.collection"], nil
}

func (d *Destination) structuredDataFormatter(data *opencdc.Data) error {
	if data == nil {
		return fmt.Errorf("data is nil")
	}
	if *data == nil {
		return nil
	}
	if sdata, ok := (*data).(opencdc.StructuredData); ok {
		*data = sdata
		return nil
	}
	raw := (*data).Bytes()
	if len(raw) == 0 {
		*data = opencdc.StructuredData{}
		return nil
	}

	m := make(map[string]interface{})
	err := json.Unmarshal(raw, &m)
	if err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}
	*data = opencdc.StructuredData(m)
	return nil
}

// getKeyColumnName will return the name of the first item in the key or the
// connector-configured default name of the key column name.
func (d *Destination) getKeyColumnName(key opencdc.Data) (string, error) {
	// Perform a type assertion to access the underlying map
	structuredKey, ok := key.(opencdc.StructuredData)
	if !ok {
		return "", fmt.Errorf("unexpected type for key: %T", key)
	}

	if len(structuredKey) > 1 {
		// Go maps aren't order preserving, so anything over len 1 will have
		// non deterministic results until we handle composite keys.
		panic("composite keys not yet supported")
	}

	// if key "id" is set (be there 1 or more keys) return id
	if _, ok := structuredKey["id"]; ok {
		return "id", nil
	}

	// TODO: this must be addressed on the source connector side as it should only be sending a single key so that we don't have to arbitrarily select here.
	// otherwise arbitrarily return the first key in the key map
	for k := range structuredKey {
		return k, nil
	}

	// return default of "id" if it reaches here, as that's surrealdb's default primary column key
	return "id", nil
}
