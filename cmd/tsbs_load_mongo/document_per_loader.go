package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	mongopkg "github.com/timescale/tsbs/pkg/targets/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	// Global counters for timing database operations
	totalDBInsertTime int64 // in nanoseconds
	totalDBCalls      int64
	totalDBRows       int64 // total rows inserted
)

// naiveBenchmark allows you to run a benchmark using the naive, one document per
// event Mongo approach
type naiveBenchmark struct {
	mongoBenchmark
}

func newNaiveBenchmark(l load.BenchmarkRunner, loaderConf *load.BenchmarkRunnerConfig) *naiveBenchmark {
	return &naiveBenchmark{mongoBenchmark{loaderConf.FileName, l, &dbCreator{}}}
}

func (b *naiveBenchmark) GetProcessor() targets.Processor {
	return &naiveProcessor{dbc: b.dbc}
}

func (b *naiveBenchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

type singlePoint struct {
	Measurement string                 `bson:"measurement"`
	Timestamp   interface{}            `bson:"timestamp_ns"` // int64 for regular collections, time.Time for time series
	Fields      map[string]interface{} `bson:"fields"`
	Tags        map[string]string      `bson:"metadata"`
}

var spPool = &sync.Pool{New: func() interface{} { return &singlePoint{} }}

type naiveProcessor struct {
	dbc        *dbCreator
	collection *mongo.Collection

	pvs []interface{}
}

func (p *naiveProcessor) Init(_ int, doLoad, _ bool) {
	if doLoad {
		db := p.dbc.client.Database(loader.DatabaseName())
		p.collection = db.Collection(collectionName)
	}
	p.pvs = []interface{}{}
}

// ProcessBatch creates a new document for each incoming event for a simpler
// approach to storing the data. This is _NOT_ the default since the aggregation method
// is recommended by Mongo and other blogs
func (p *naiveProcessor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch).arr
	if cap(p.pvs) < len(batch) {
		p.pvs = make([]interface{}, len(batch))
	}
	p.pvs = p.pvs[:len(batch)]
	var metricCnt uint64
	for i, event := range batch {
		var doc interface{}

		if useTimeSeries {
			// For time series collections, flatten measurements to top level
			// This is optimal for MongoDB time series collections and high cardinality indexing
			flatDoc := bson.M{
				"measurement":  string(event.MeasurementName()),
				"timestamp_ns": time.Unix(0, event.Timestamp()).UTC(),
				"metadata":     map[string]string{},
			}

			// Add all fields at the top level (not nested)
			f := &mongopkg.MongoReading{}
			for j := 0; j < event.FieldsLength(); j++ {
				event.Fields(f, j)
				flatDoc[string(f.Key())] = f.Value()
			}

			// Add tags to metadata
			t := &mongopkg.MongoTag{}
			metadata := flatDoc["metadata"].(map[string]string)
			for j := 0; j < event.TagsLength(); j++ {
				event.Tags(t, j)
				metadata[string(t.Key())] = string(t.Value())
			}

			doc = flatDoc
			metricCnt += uint64(event.FieldsLength())
		} else {
			// For regular collections, use the nested structure with fields object
			x := spPool.Get().(*singlePoint)
			x.Measurement = string(event.MeasurementName())
			x.Timestamp = event.Timestamp()
			x.Fields = map[string]interface{}{}
			x.Tags = map[string]string{}

			f := &mongopkg.MongoReading{}
			for j := 0; j < event.FieldsLength(); j++ {
				event.Fields(f, j)
				x.Fields[string(f.Key())] = f.Value()
			}
			t := &mongopkg.MongoTag{}
			for j := 0; j < event.TagsLength(); j++ {
				event.Tags(t, j)
				x.Tags[string(t.Key())] = string(t.Value())
			}

			doc = x
			metricCnt += uint64(event.FieldsLength())
		}

		p.pvs[i] = doc
	}

	if doLoad {
		ctx, cancel := context.WithTimeout(p.dbc.ctx, writeTimeout)
		defer cancel()

		// Measure only the database insertion time
		dbStart := time.Now()
		_, err := p.collection.InsertMany(ctx, p.pvs)
		dbDuration := time.Since(dbStart)

		// Accumulate timing statistics
		atomic.AddInt64(&totalDBInsertTime, dbDuration.Nanoseconds())
		atomic.AddInt64(&totalDBCalls, 1)
		atomic.AddInt64(&totalDBRows, int64(len(batch)))

		if err != nil {
			log.Fatalf("Bulk insert docs err: %s\n", err.Error())
		}
	}

	// Only return singlePoint objects to the pool (not bson.M objects used for time series)
	if !useTimeSeries {
		for _, p := range p.pvs {
			spPool.Put(p)
		}
	}

	// Return metricCnt (total field values) and rowCnt (number of documents/events inserted)
	return metricCnt, uint64(len(batch))
}

// PrintDBTimingStats prints the accumulated database timing statistics
func PrintDBTimingStats() {
	totalCalls := atomic.LoadInt64(&totalDBCalls)
	totalTime := atomic.LoadInt64(&totalDBInsertTime)
	totalRows := atomic.LoadInt64(&totalDBRows)

	if totalCalls > 0 {
		avgTimeMs := float64(totalTime) / float64(totalCalls) / 1e6
		totalTimeSec := float64(totalTime) / 1e9
		meanRowsPerSec := float64(totalRows) / totalTimeSec

		fmt.Printf("\n=== Database Insert Timing Statistics ===\n")
		fmt.Printf("Total DB InsertMany calls: %d\n", totalCalls)
		fmt.Printf("Total rows inserted: %d\n", totalRows)
		fmt.Printf("Total time in DB operations: %.3f sec\n", totalTimeSec)
		fmt.Printf("Average time per InsertMany call: %.3f ms\n", avgTimeMs)
		fmt.Printf("Mean insert rate: %.2f rows/sec\n", meanRowsPerSec)
		fmt.Printf("=========================================\n\n")
	}
}
