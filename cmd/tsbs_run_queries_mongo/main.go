// tsbs_run_queries_mongo speed tests Mongo using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided Mongo endpoint using the official MongoDB driver.
package main

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"time"

	"github.com/blagojts/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Program option vars:
var (
	daemonURL string
	timeout   time.Duration
)

// Global vars:
var (
	runner *query.BenchmarkRunner
	client *mongo.Client
	ctx    context.Context
)

// Parse args:
func init() {
	// needed for deserializing the mongo query from gob
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register(bson.M{})
	gob.Register([]bson.M{})

	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("url", "mongodb://localhost:27017", "Daemon URL.")
	pflag.Duration("read-timeout", 30*time.Second, "Timeout value for individual queries")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	daemonURL = viper.GetString("url")
	timeout = viper.GetDuration("read-timeout")

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	var err error
	ctx = context.Background()

	clientOptions := options.Client().
		ApplyURI(daemonURL).
		SetConnectTimeout(timeout).
		SetServerSelectionTimeout(timeout)

	client, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Printf("Error disconnecting: %v", err)
		}
	}()

	// Verify connection
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err = client.Ping(pingCtx, nil)
	if err != nil {
		log.Fatal(err)
	}

	runner.Run(&query.MongoPool, newProcessor)
}

type processor struct {
	collection *mongo.Collection
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	db := client.Database(runner.DatabaseName())
	p.collection = db.Collection("point_data")
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	mq := q.(*query.Mongo)
	start := time.Now().UnixNano()

	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Convert pipeline to use allowDiskUse option
	opts := options.Aggregate().SetAllowDiskUse(true)

	if runner.DebugLevel() > 0 {
		fmt.Println(mq.BsonDoc)
	}

	cursor, err := p.collection.Aggregate(queryCtx, mq.BsonDoc, opts)
	if err != nil {
		took := time.Now().UnixNano() - start
		lag := float64(took) / 1e6
		stat := query.GetStat()
		stat.Init(q.HumanLabelName(), lag)
		return []*query.Stat{stat}, err
	}
	defer cursor.Close(queryCtx)

	var result map[string]interface{}
	cnt := 0
	for cursor.Next(queryCtx) {
		err := cursor.Decode(&result)
		if err != nil {
			log.Printf("Error decoding result: %v", err)
			continue
		}
		if runner.DoPrintResponses() {
			fmt.Printf("ID %d: %v\n", q.GetID(), result)
		}
		cnt++
	}

	if runner.DebugLevel() > 0 {
		fmt.Println(cnt)
	}

	if err := cursor.Err(); err != nil {
		took := time.Now().UnixNano() - start
		lag := float64(took) / 1e6
		stat := query.GetStat()
		stat.Init(q.HumanLabelName(), lag)
		return []*query.Stat{stat}, err
	}

	took := time.Now().UnixNano() - start
	lag := float64(took) / 1e6 // milliseconds
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}
