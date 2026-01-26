package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type dbCreator struct {
	client *mongo.Client
	ctx    context.Context
}

func (d *dbCreator) Init() {
	var err error
	d.ctx = context.Background()

	clientOptions := options.Client().
		ApplyURI(daemonURL).
		SetConnectTimeout(writeTimeout).
		SetServerSelectionTimeout(writeTimeout)

	d.client, err = mongo.Connect(d.ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Verify connection
	ctx, cancel := context.WithTimeout(d.ctx, writeTimeout)
	defer cancel()
	err = d.client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func (d *dbCreator) DBExists(dbName string) bool {
	ctx, cancel := context.WithTimeout(d.ctx, writeTimeout)
	defer cancel()

	dbs, err := d.client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	for _, name := range dbs {
		if name == dbName {
			return true
		}
	}
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {

	ctx, cancel := context.WithTimeout(d.ctx, writeTimeout)
	defer cancel()

	db := d.client.Database(dbName)
	collections, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return err
	}
	for _, name := range collections {
		err := db.Collection(name).Drop(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	ctx, cancel := context.WithTimeout(d.ctx, writeTimeout)
	defer cancel()

	db := d.client.Database(dbName)

	var err error

	if useTimeSeries {
		// Create MongoDB native time series collection using raw command
		// This allows us to pass hcindex options that may not be in the Go driver yet
		timeseriesConfig := bson.D{
			{Key: "timeField", Value: timestampField},
			{Key: "metaField", Value: "metadata"},
			{Key: "granularity", Value: "seconds"},
		}

		if useHCIndex {
			// Add high cardinality index options
			timeseriesConfig = append(timeseriesConfig, bson.E{Key: "useHCIndex", Value: true})

			hcindexOpts := bson.D{
				{Key: "period", Value: "hour"},
				{Key: "frequency", Value: int32(6)},
			}

			// Add excludedColumns for high cardinality fields
			// These are fields that should not be indexed due to very high cardinality
			excludedCols := bson.A{"order_id", "session_id", "cart_id", "primary_product_id", "user_id"}
			hcindexOpts = append(hcindexOpts, bson.E{Key: "excludedColumns", Value: excludedCols})

			timeseriesConfig = append(timeseriesConfig, bson.E{Key: "hcindexOptions", Value: hcindexOpts})
			log.Printf("Creating MongoDB time series collection with high cardinality index")
		} else {
			log.Printf("Creating MongoDB time series collection (requires MongoDB 5.0+)")
		}

		createCmd := bson.D{
			{Key: "create", Value: collectionName},
			{Key: "timeseries", Value: timeseriesConfig},
		}

		// Debug: log the exact command being sent
		if cmdBytes, marshalErr := bson.MarshalExtJSON(createCmd, false, false); marshalErr == nil {
			log.Printf("Creating collection with command: %s", string(cmdBytes))
		}

		err = db.RunCommand(ctx, createCmd).Err()
	} else {
		// Create regular collection with wiredtiger settings
		createOpts := options.CreateCollection()
		createOpts.SetStorageEngine(bson.M{
			"wiredTiger": bson.M{
				"configString": "block_compressor=snappy",
			},
		})
		err = db.CreateCollection(ctx, collectionName, createOpts)
	}
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			// Collection already exists, continue
		} else {
			return fmt.Errorf("create collection err: %v", err)
		}
	}

	collection := db.Collection(collectionName)

	// Time series collections have automatic indexing on timeField and metaField
	// so we only create custom indexes for regular collections
	if !useTimeSeries {
		// Create index
		var keys bson.D
		if documentPer {
			keys = bson.D{
				{Key: "measurement", Value: 1},
				{Key: "tags.hostname", Value: 1},
				{Key: timestampField, Value: 1},
			}
		} else {
			keys = bson.D{
				{Key: aggKeyID, Value: 1},
				{Key: "measurement", Value: 1},
				{Key: "tags.hostname", Value: 1},
			}
		}

		indexModel := mongo.IndexModel{
			Keys: keys,
			Options: options.Index().
				SetUnique(false).
				SetBackground(false).
				SetSparse(false),
		}

		ctx2, cancel2 := context.WithTimeout(d.ctx, writeTimeout)
		defer cancel2()
		_, err = collection.Indexes().CreateOne(ctx2, indexModel)
		if err != nil {
			return fmt.Errorf("create basic index err: %v", err)
		}

		// To make updates for new records more efficient, we need an efficient doc
		// lookup index
		if !documentPer {
			aggIndexModel := mongo.IndexModel{
				Keys: bson.D{{Key: aggDocID, Value: 1}},
				Options: options.Index().
					SetUnique(false).
					SetBackground(false).
					SetSparse(false),
			}

			ctx3, cancel3 := context.WithTimeout(d.ctx, writeTimeout)
			defer cancel3()
			_, err = collection.Indexes().CreateOne(ctx3, aggIndexModel)
			if err != nil {
				return fmt.Errorf("create agg doc index err: %v", err)
			}
		}
	} else {
		log.Printf("Skipping custom index creation for time series collection (automatic indexing enabled)")
	}

	return nil
}

// Note: We intentionally do NOT implement the Close() method here.
// The MongoDB client connection needs to remain open for the duration of the benchmark
// as worker processors use the same dbCreator.client reference.
// The connection will be cleaned up when the process exits.
