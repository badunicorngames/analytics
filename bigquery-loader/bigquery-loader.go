package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

var (
	project    = flag.String("project", "bad-unicorn-development", "Google Cloud Project ID")
	gcs_bucket = flag.String("gcs_bucket", "badunicorn-analytics", "GCS bucket containing the event files")
	gcs_path   = flag.String("gcs_path", "raw", "Path to the event files, like: gs://{gcs_bucket}/{gcs_path}/2016/...")
	gcs_prefix = flag.String("gcs_prefix", "analytics-h", "Filename prefix on GCS before the hour (analytics-h10 is hour 10 with prefix 'analytics-h')")
	bq_dataset = flag.String("bq_dataset", "badunicorn_analytics", "Dataset containing the raw events table (bq_table)")
	bq_table   = flag.String("bq_table", "raw_events", "Table containing the raw events")
)

var ctx context.Context
var bq_client *bigquery.Client

func main() {
	ctx = context.Background()

	bq_latest := getMostRecentEventTime().UTC()
	now := time.Now().UTC()
	log.Printf("Uploading files between %s and %s", bq_latest.String(), now.String())

	uploadFilesBetween(bq_latest, now)
}

func getMostRecentEventTime() time.Time {
	var err error
	bq_client, err = bigquery.NewClient(ctx, *project)
	if err != nil {
		log.Fatalf("Failed to create bigquery client: %v", err)
	}

	dataset := bq_client.Dataset(*bq_dataset)
	table := dataset.Table(*bq_table)

	var queryString = fmt.Sprintf("SELECT MAX(time) FROM [%s]", table.FullyQualifiedName())
	q := bq_client.Query(queryString)
	it, err := q.Read(ctx)
	if err != nil {
		log.Println("Query failed: " + err.Error())
	}
	var result time.Time
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Println("Query result iterator failed: " + err.Error())
		}
		if len(row) >= 1 {
			eventTimeString := row[0]
			eventTime, err := time.Parse("2006-01-02 15:04:05.000000000 -0700 MST", eventTimeString.(string))
			if err == nil && eventTime.After(result) {
				result = eventTime
			}
		}
		fmt.Println(row)
	}
	return result
}

func uploadFilesBetween(earliest time.Time, latest time.Time) {

	// Setup

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	bucket := client.Bucket(*gcs_bucket)

	var query storage.Query
	if gcs_path != nil {
		query.Prefix = *gcs_path
	}
	query.Versions = false

	// Query all objects under gcs_path

	var objectsInRange []*storage.ObjectAttrs
	iter := bucket.Objects(ctx, &query)
	for {
		objAttrs, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Println("Iterator error: " + err.Error())
			continue
		}

		// Convert the gcs object path+name to a usable datetime
		formatString := *gcs_path + "/%d/%d/%d/" + *gcs_prefix + "%d"
		var objYear, objMonth, objDay, objHour int
		fmt.Sscanf(objAttrs.Name, formatString, &objYear, &objMonth, &objDay, &objHour)
		objTime := time.Date(objYear, time.Month(objMonth), objDay, objHour, 0, 0, 0, time.UTC)

		// Does this object correspond to one in the range we're looking for?
		if objTime.After(earliest) && objTime.Before(latest) {
			objectsInRange = append(objectsInRange, objAttrs)
		}
	}

	numObjects := len(objectsInRange)
	log.Printf("Found %d objects in range", numObjects)

	if numObjects > 0 {
		// Upload them to BigQuery

		dataset := bq_client.Dataset(*bq_dataset)
		table := dataset.Table(*bq_table)
		for _, obj := range objectsInRange {
			gcs_filepath := fmt.Sprintf("gs://%s/%s", *gcs_bucket, obj.Name)
			gcsRef := bigquery.NewGCSReference(gcs_filepath)
			gcsRef.SourceFormat = bigquery.Avro

			loader := table.LoaderFrom(gcsRef)
			loader.CreateDisposition = bigquery.CreateNever
			loader.WriteDisposition = bigquery.WriteAppend

			log.Printf("Loading %s into BigQuery...", gcs_filepath)
			job, err := loader.Run(ctx)
			if err != nil {
				panic("loader.Run failed: " + err.Error())
			}

			status, err := job.Wait(ctx)
			if err != nil {
				panic("job failed: " + err.Error())
			}
			if err := status.Err(); err != nil {
				panic("job failed: " + err.Error())
			}

			log.Println("Done")
		}

		log.Println("Finished uploading files")
	} else {
		log.Println("Nothing to do")
	}
}
