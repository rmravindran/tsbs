package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
	mongopkg "github.com/timescale/tsbs/pkg/targets/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	// Global counters for timing database operations in aggregate mode
	totalAggDBInsertTime  int64 // in nanoseconds (for InsertMany)
	totalAggDBBulkTime    int64 // in nanoseconds (for BulkWrite)
	totalAggDBInsertCalls int64
	totalAggDBBulkCalls   int64
	totalAggDBRows        int64 // total rows processed
)

type hostnameIndexer struct {
	partitions uint
}

func (i *hostnameIndexer) GetIndex(item data.LoadedPoint) uint {
	p := item.Data.(*mongopkg.MongoPoint)
	t := &mongopkg.MongoTag{}
	for j := 0; j < p.TagsLength(); j++ {
		p.Tags(t, j)
		key := string(t.Key())
		if key == "hostname" || key == "name" {
			// the hostame is the defacto index for devops tags
			// the truck name is the defacto index for iot tags
			h := fnv.New32a()
			h.Write([]byte(string(t.Value())))
			return uint(h.Sum32()) % i.partitions
		}
	}
	// name tag may be skipped in iot use-case
	return 0
}

// aggBenchmark allows you to run a benchmark using the aggregated document format
// for Mongo
type aggBenchmark struct {
	mongoBenchmark
}

func newAggBenchmark(l load.BenchmarkRunner, conf *load.BenchmarkRunnerConfig) *aggBenchmark {
	// Pre-create the needed empty subdoc for new aggregate docs
	generateEmptyHourDoc()

	return &aggBenchmark{mongoBenchmark{conf.FileName, l, &dbCreator{}}}
}

func (b *aggBenchmark) GetProcessor() targets.Processor {
	return &aggProcessor{dbc: b.dbc}
}

func (b *aggBenchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &hostnameIndexer{partitions: maxPartitions}
}

// point is a reusable data structure to store a BSON data document for Mongo,
// that can then be manipulated for bookkeeping and final document preparation
type point struct {
	Timestamp int64                  `bson:"timestamp_ns"`
	Fields    map[string]interface{} `bson:"fields"`
}

var emptyDoc [][]bson.M

func generateEmptyHourDoc() {
	emptyDoc = make([][]bson.M, 60)
	for j := range emptyDoc {
		emptyDoc[j] = make([]bson.M, 60)
	}
}

var pPool = &sync.Pool{New: func() interface{} { return &point{} }}

type aggProcessor struct {
	dbc        *dbCreator
	collection *mongo.Collection

	createdDocs map[string]bool
	createQueue []interface{}
}

func (p *aggProcessor) Init(_ int, doLoad, _ bool) {
	if doLoad {
		db := p.dbc.client.Database(loader.DatabaseName())
		p.collection = db.Collection(collectionName)
	}
	p.createdDocs = make(map[string]bool)
	p.createQueue = []interface{}{}

}

// ProcessBatch receives a batch of bson.M documents (BSON maps) that
// each correspond to a datapoint and puts the points in the appropriate aggregated
// document. Documents are aggregated on a per-sensor, per-hour basis, meaning
// each document can hold up to 3600 readings (one per second) that only need
// to be updated after initial creation (when the new per-sensor, per-host combination
// is first encountered)
//
// A document is structured like so:
//
//	 {
//	   "doc_id": "day_x_00",
//	   "key_id": "x_00",
//	   "measurement": "cpu",
//	   "tags": {
//	     "hostname": "host0",
//	     ...
//	   },
//	   "events": [
//	     [
//	       {
//	         "field1": 0.0,
//	         ...
//			  }
//	     ]
//	   ]
//	 }
func (p *aggProcessor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	docToEvents := make(map[string][]*point)
	batch := b.(*batch)

	eventCnt := uint64(0)
	for _, event := range batch.arr {
		tagsMap := map[string]string{}
		t := &mongopkg.MongoTag{}
		for j := 0; j < event.TagsLength(); j++ {
			event.Tags(t, j)
			tagsMap[string(t.Key())] = string(t.Value())
		}

		// Determine which document this event belongs too
		ts := event.Timestamp()
		dateKey := time.Unix(0, ts).UTC().Format(aggDateFmt)
		docKey := fmt.Sprintf("day_%s_%s_%s", tagsMap["hostname"], dateKey, string(event.MeasurementName()))

		// Check that it has been created using a cached map, if not, add
		// to creation queue
		_, ok := p.createdDocs[docKey]
		if !ok {
			if _, ok := p.createdDocs[docKey]; !ok {
				p.createQueue = append(p.createQueue, bson.M{
					aggDocID:      docKey,
					aggKeyID:      dateKey,
					"measurement": string(event.MeasurementName()),
					"tags":        tagsMap,
					"events":      emptyDoc,
				})
			}
			p.createdDocs[docKey] = true
		}

		// Cache events to be updated on a per-document basis for efficient
		// batching later
		if _, ok := docToEvents[docKey]; !ok {
			docToEvents[docKey] = []*point{}
		}
		x := pPool.Get().(*point)
		x.Fields = map[string]interface{}{}
		f := &mongopkg.MongoReading{}
		for j := 0; j < event.FieldsLength(); j++ {
			event.Fields(f, j)
			x.Fields[string(f.Key())] = f.Value()
		}
		x.Timestamp = ts
		eventCnt += uint64(len(x.Fields))

		docToEvents[docKey] = append(docToEvents[docKey], x)
	}

	// Count the number of events (rows) being processed
	rowCnt := uint64(0)
	for _, events := range docToEvents {
		rowCnt += uint64(len(events))
	}

	if doLoad {
		ctx, cancel := context.WithTimeout(p.dbc.ctx, writeTimeout)
		defer cancel()

		// Checks if any new documents need to be made and does so
		insertNewAggregateDocs(ctx, p.collection, p.createQueue)
		p.createQueue = p.createQueue[:0]

		// For each document, create one 'set' command for all records
		// that belong to the document
		var updates []mongo.WriteModel
		for docKey, events := range docToEvents {
			filter := bson.M{aggDocID: docKey}
			updateMap := bson.M{}
			for _, event := range events {
				minKey := (event.Timestamp / (1e9 * 60)) % 60
				secKey := (event.Timestamp / 1e9) % 60
				key := fmt.Sprintf("events.%d.%d", minKey, secKey)
				val := event.Fields

				val[timestampField] = event.Timestamp
				updateMap[key] = val
			}

			update := bson.M{"$set": updateMap}
			updateModel := mongo.NewUpdateOneModel().
				SetFilter(filter).
				SetUpdate(update)
			updates = append(updates, updateModel)
		}

		// All documents accounted for, finally run the operation
		if len(updates) > 0 {
			// Measure only the database BulkWrite time
			dbStart := time.Now()
			_, err := p.collection.BulkWrite(ctx, updates)
			dbDuration := time.Since(dbStart)

			// Accumulate timing statistics
			atomic.AddInt64(&totalAggDBBulkTime, dbDuration.Nanoseconds())
			atomic.AddInt64(&totalAggDBBulkCalls, 1)
			atomic.AddInt64(&totalAggDBRows, int64(rowCnt))

			if err != nil {
				log.Fatalf("Bulk aggregate update err: %s\n", err.Error())
			}
		}

		for _, events := range docToEvents {
			for _, e := range events {
				delete(e.Fields, timestampField)
				pPool.Put(e)
			}
		}
	}
	// Return eventCnt (total field values) and rowCnt (number of events/readings processed)
	return eventCnt, rowCnt
}

// insertNewAggregateDocs handles creating new aggregated documents when new devices
// or time periods are encountered
func insertNewAggregateDocs(ctx context.Context, collection *mongo.Collection, createQueue []interface{}) {
	if len(createQueue) > 0 {
		off := 0
		for off < len(createQueue) {
			l := off + aggInsertBatchSize
			if l > len(createQueue) {
				l = len(createQueue)
			}

			// Measure only the database InsertMany time
			dbStart := time.Now()
			_, err := collection.InsertMany(ctx, createQueue[off:l])
			dbDuration := time.Since(dbStart)

			// Accumulate timing statistics
			atomic.AddInt64(&totalAggDBInsertTime, dbDuration.Nanoseconds())
			atomic.AddInt64(&totalAggDBInsertCalls, 1)

			if err != nil {
				log.Fatalf("Bulk aggregate docs err: %s\n", err.Error())
			}

			off = l
		}
	}
}

// PrintAggDBTimingStats prints the accumulated database timing statistics for aggregate mode
func PrintAggDBTimingStats() {
	totalInsertCalls := atomic.LoadInt64(&totalAggDBInsertCalls)
	totalInsertTime := atomic.LoadInt64(&totalAggDBInsertTime)
	totalBulkCalls := atomic.LoadInt64(&totalAggDBBulkCalls)
	totalBulkTime := atomic.LoadInt64(&totalAggDBBulkTime)
	totalRows := atomic.LoadInt64(&totalAggDBRows)

	fmt.Printf("\n=== Database Timing Statistics (Aggregate Mode) ===\n")

	if totalInsertCalls > 0 {
		avgInsertTimeMs := float64(totalInsertTime) / float64(totalInsertCalls) / 1e6
		totalInsertTimeSec := float64(totalInsertTime) / 1e9
		fmt.Printf("InsertMany operations:\n")
		fmt.Printf("  Total calls: %d\n", totalInsertCalls)
		fmt.Printf("  Total time: %.3f sec\n", totalInsertTimeSec)
		fmt.Printf("  Average time per call: %.3f ms\n", avgInsertTimeMs)
	}

	if totalBulkCalls > 0 {
		avgBulkTimeMs := float64(totalBulkTime) / float64(totalBulkCalls) / 1e6
		totalBulkTimeSec := float64(totalBulkTime) / 1e9
		fmt.Printf("BulkWrite operations:\n")
		fmt.Printf("  Total calls: %d\n", totalBulkCalls)
		fmt.Printf("  Total time: %.3f sec\n", totalBulkTimeSec)
		fmt.Printf("  Average time per call: %.3f ms\n", avgBulkTimeMs)
	}

	if totalInsertCalls > 0 || totalBulkCalls > 0 {
		totalDBTime := float64(totalInsertTime+totalBulkTime) / 1e9
		fmt.Printf("Total rows processed: %d\n", totalRows)
		fmt.Printf("Total DB operation time: %.3f sec\n", totalDBTime)
		if totalDBTime > 0 {
			meanRowsPerSec := float64(totalRows) / totalDBTime
			fmt.Printf("Mean insert rate: %.2f rows/sec\n", meanRowsPerSec)
		}
	}

	fmt.Printf("====================================================\n\n")
}
