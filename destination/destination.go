package destination

import (
	"context"
	"encoding/json"
	"time"

	//"strings"

	// "encoding/json"
	"fmt"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/surrealdb/surrealdb.go"
	"github.com/surrealdb/surrealdb.go/pkg/models"
)

type Destination struct {
	sdk.UnimplementedDestination

	config Config
	db     *surrealdb.DB
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

type User struct {
	ID      int    `json:"id,omitempty"`
	Name    string `json:"name"`
	Surname string `json:"surname"`
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
	} else {
		sdk.Logger(ctx).Info().Msg("Created SurrealDB client")
	}

	authData := &models.Auth{
		Username: d.config.Username,
		Password: d.config.Password,
		// Database:  d.config.Database,
		// Namespace: d.config.Namespace,
		// Scope:     d.config.Scope,
	}
	if _, err = db.Signin(authData); err != nil {
		sdk.Logger(ctx).Error().Msg("Failed to sign in to SurrealDB: " + err.Error())
		return fmt.Errorf("failed to sign in to SurrealDB: %w", err)
	} else {
		sdk.Logger(ctx).Info().Msg("Signed in to SurrealDB")
	}

	sdk.Logger(ctx).Info().Msg("Try to use namespace and database: " + d.config.Namespace + " " + d.config.Database)

	if _, err = db.Use(d.config.Namespace, d.config.Database); err != nil {
		sdk.Logger(ctx).Error().Msg("Failed to select namespace and database: " + err.Error())
		return fmt.Errorf("failed to select namespace and database: %w", err)
	} else {
		sdk.Logger(ctx).Info().Msg("Using namespace and database: " + d.config.Namespace + " " + d.config.Database)
	}
	sdk.Logger(ctx).Info().Msg("Successfully connected to SurrealDB")

	// ********** sample queries to test connection *****************************************************

	/* // Define user struct
	user := User{
		Name:    "John",
		Surname: "Doe",
	}
	*/

	// Change userID to be a string directly
	// userID := fmt.Sprintf("%d", rand.Intn(1000)+1)
	/* userID := rand.Intn(100000000000) + 1
	user := User{
		Name:    "John",
		Surname: "Doe",
		ID:      userID,
		// ID: 769876,
	}

	// Insert user
	_, err = db.Insert("user", user)
	if err != nil {
		sdk.Logger(ctx).Error().Msg("Failed to create user: " + err.Error())
		return fmt.Errorf("failed to create user: %w", err)
	} */
	//  else {

	// 	// [map
	// 	// [
	// 	// 	id:{
	// 	// 		Number:8
	// 	// 		Content:[user 4728]
	// 	// 	}
	// 	// 	name:John
	// 	// 	surname:Doe
	// 	// 	]
	// 	// ]
	// 	// Unmarshal data
	// 	// createdUser := make([]User, 1)
	// 	// err = marshal.Unmarshal(response, &createdUser)
	// 	// if err != nil {
	// 	// 	// log the error
	// 	// 	sdk.Logger(ctx).Error().Msg("Failed to unmarshal created user data: " + err.Error())
	// 	// 	panic(err)
	// 	// }
	// 	// // log the created user
	// 	// sdk.Logger(ctx).Info().Msg(fmt.Sprintf("Created user: %v", createdUser))

	// 	/* // Convert response to map[string]interface{}
	// 	convertedResponse := make(map[string]interface{})
	// 	if respMap, ok := response.(map[string]interface{}); ok {
	// 		for key, value := range respMap {
	// 			strKey := fmt.Sprintf("%v", key)
	// 			convertedResponse[strKey] = value
	// 		}
	// 	} else {
	// 		sdk.Logger(ctx).Error().Msg("Failed to assert response type")
	// 	}

	// 	responseJSON, err := json.Marshal(convertedResponse)
	// 	if err != nil {
	// 		sdk.Logger(ctx).Error().Msg("Failed to marshal response: " + err.Error())
	// 	} else {
	// 		sdk.Logger(ctx).Info().Msg("Response: " + string(responseJSON))
	// 	} */
	// }

	// sdk.Logger(ctx).Info().Msg(fmt.Sprintf("Created user: %d", user.ID))

	/* // log data response
	sdk.Logger(ctx).Info().Msg(fmt.Sprintf("Data response: %v", data))
	*/
	// Unmarshal data
	/* user, err = marshal.SmartUnmarshal[User](marshal.SmartMarshal(s.db.Create, user)) */

	// data, err = marshal.SmartUnmarshal[User](db.Select(user[0].ID))

	// createdUser, err = marshal.SmartUnmarshal(data, err)
	// if err != nil {
	// 	sdk.Logger(ctx).Error().Msg("Failed to unmarshal created user data: " + err.Error())
	// 	return fmt.Errorf("failed to unmarshal created user data: %w", err)
	// }

	// Unmarshal data
	// selectedUser := new(User)
	// err = marshal.Unmarshal(data, &selectedUser)
	// if err != nil {
	// 	sdk.Logger(ctx).Error().Msg("Failed to unmarshal selected user data: " + err.Error())
	// 	return fmt.Errorf("failed to unmarshal selected user data: %w", err)
	// }

	// Change part/parts of user
	/* changes := map[string]string{"name": "Jane"}

	// Update user
	if _, err = db.Update(user.ID, changes); err != nil {
		sdk.Logger(ctx).Error().Msg("Failed to update user: " + err.Error())
		return fmt.Errorf("failed to update user: %w", err)
	} else {
		sdk.Logger(ctx).Info().Msg("Updated user: " + user.ID + " with changes name: " + changes["name"])
	} */

	/* if _, err = db.Query("UPDATE user:"+fmt.Sprintf("%d", user.ID)+" SET name = 'Jane'", nil); err != nil {

		sdk.Logger(ctx).Error().Msg("Failed to update user: " + err.Error())
		return fmt.Errorf("failed to update user: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("Updated user: " + fmt.Sprintf("user:%d", user.ID) + " with changes name: " + user.Name) */

	// // ID      models.RecordID `json:"id,omitempty"`
	// type User2 struct {
	// 	Name    string `json:"name"`
	// 	Surname string `json:"surname"`
	// }

	// user2 := User2{
	// 	Name:    "Jane",
	// 	Surname: "Smith",
	// }

	// // id := *models.NewRecordID("user", user.ID)
	// id := "user:769876"
	// sdk.Logger(ctx).Info().Msg(fmt.Sprintf("Generated RecordID: %v", id))

	// response, err := db.Update(id, user2)
	// if err != nil {
	// 	sdk.Logger(ctx).Error().Msg("Failed to update user: " + err.Error())
	// 	return fmt.Errorf("failed to update user: %w", err)
	// }

	// // log the response
	// responseJSON, err := json.Marshal(response)
	// if err != nil {
	// 	sdk.Logger(ctx).Error().Msg("Failed to marshal response: " + err.Error())
	// } else {
	// 	sdk.Logger(ctx).Info().Msg("Response: " + string(responseJSON))
	// }

	/* if selectedUser, err := db.Query("SELECT * FROM "+user.ID, nil); err != nil {

		sdk.Logger(ctx).Error().Msg("Failed to query user: " + err.Error())
		return fmt.Errorf("failed to query user: %w", err)
	} else {
		sdk.Logger(ctx).Info().Msg("Queried user: " + selectedUser.String())
	} */

	// ********** end of sample queries to test connection **********

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
		return d.db.Close()
	}
	return nil
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
	if _, err := d.db.Insert(tableName, payloads); err != nil {
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
		if _, err := d.db.Update(tableName, payload); err != nil {
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

		if _, err := d.db.Delete(tableName); err != nil {
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
