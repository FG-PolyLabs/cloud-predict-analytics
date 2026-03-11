package polymarket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
)

// BQLoader writes prediction snapshots to BigQuery.
// MergeSnapshots uses a staging table + MERGE so re-running the job never
// produces duplicate rows — only (market_id, timestamp) pairs that don't
// already exist in the target table are inserted.
type BQLoader struct {
	client    *bigquery.Client
	projectID string
	datasetID string
	tableID   string
}

// NewBQLoader creates a loader using Application Default Credentials.
func NewBQLoader(ctx context.Context, projectID, datasetID, tableID string) (*BQLoader, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("creating BigQuery client: %w", err)
	}
	return &BQLoader{
		client:    client,
		projectID: projectID,
		datasetID: datasetID,
		tableID:   tableID,
	}, nil
}

// Close releases the underlying BigQuery client.
func (l *BQLoader) Close() error {
	return l.client.Close()
}

// MergeSnapshots loads snapshots into a short-lived staging table, then runs
// a MERGE into the target table that only inserts rows where the combination
// of (market_id, timestamp) does not already exist.
// Returns the number of new rows inserted.
func (l *BQLoader) MergeSnapshots(ctx context.Context, snapshots []PredictionSnapshot) (int, error) {
	if len(snapshots) == 0 {
		return 0, nil
	}

	// Encode snapshots as JSONL — json tags on PredictionSnapshot match BQ column names.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, s := range snapshots {
		if err := enc.Encode(s); err != nil {
			return 0, fmt.Errorf("encoding snapshot: %w", err)
		}
	}

	// Create a temporary staging table that auto-expires after 1 hour.
	stagingID := fmt.Sprintf("staging_%d", time.Now().UnixNano())
	staging := l.client.Dataset(l.datasetID).Table(stagingID)
	if err := staging.Create(ctx, &bigquery.TableMetadata{
		Schema:         snapshotSchema(),
		ExpirationTime: time.Now().Add(time.Hour),
	}); err != nil {
		return 0, fmt.Errorf("creating staging table: %w", err)
	}
	defer staging.Delete(ctx) //nolint:errcheck // best-effort cleanup

	// Batch-load the JSONL into the staging table.
	// Load jobs are fully committed before returning, so the MERGE below will
	// always see all rows — no streaming-buffer delay.
	src := bigquery.NewReaderSource(bytes.NewReader(buf.Bytes()))
	src.SourceFormat = bigquery.JSON
	src.Schema = snapshotSchema()

	loadJob, err := staging.LoaderFrom(src).Run(ctx)
	if err != nil {
		return 0, fmt.Errorf("starting staging load: %w", err)
	}
	if _, err := loadJob.Wait(ctx); err != nil {
		return 0, fmt.Errorf("staging load failed: %w", err)
	}

	// MERGE: insert only rows whose (market_id, timestamp) key is new.
	// The target table is day-partitioned on `date` so this only scans the
	// relevant day partition — not the whole table.
	mergeSQL := fmt.Sprintf(
		"MERGE `%s.%s.%s` T "+
			"USING `%s.%s.%s` S "+
			"ON T.market_id = S.market_id AND T.timestamp = S.timestamp "+
			"WHEN NOT MATCHED THEN INSERT ROW",
		l.projectID, l.datasetID, l.tableID,
		l.projectID, l.datasetID, stagingID,
	)

	mergeJob, err := l.client.Query(mergeSQL).Run(ctx)
	if err != nil {
		return 0, fmt.Errorf("starting MERGE: %w", err)
	}
	status, err := mergeJob.Wait(ctx)
	if err != nil {
		return 0, fmt.Errorf("MERGE job failed: %w", err)
	}
	if err := status.Err(); err != nil {
		return 0, fmt.Errorf("MERGE error: %w", err)
	}

	if qs, ok := status.Statistics.Details.(*bigquery.QueryStatistics); ok {
		return int(qs.NumDMLAffectedRows), nil
	}
	return 0, nil
}

// snapshotSchema returns the BigQuery schema for PredictionSnapshot.
// Must stay in sync with the table definition in fg-polylabs.weather.polymarket_snapshots.
func snapshotSchema() bigquery.Schema {
	req := func(name string, t bigquery.FieldType) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Name: name, Type: t, Required: true}
	}
	opt := func(name string, t bigquery.FieldType) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Name: name, Type: t}
	}
	return bigquery.Schema{
		req("city",              bigquery.StringFieldType),
		req("date",              bigquery.DateFieldType),
		req("timestamp",         bigquery.TimestampFieldType),
		req("temp_threshold",    bigquery.FloatFieldType),
		req("yes_cost",          bigquery.FloatFieldType),
		req("no_cost",           bigquery.FloatFieldType),
		opt("best_bid",          bigquery.FloatFieldType),
		opt("best_ask",          bigquery.FloatFieldType),
		opt("spread",            bigquery.FloatFieldType),
		opt("volume_24h",        bigquery.FloatFieldType),
		opt("volume_total",      bigquery.FloatFieldType),
		opt("liquidity",         bigquery.FloatFieldType),
		req("market_id",         bigquery.StringFieldType),
		req("event_slug",        bigquery.StringFieldType),
		opt("market_end_date",   bigquery.StringFieldType),
		opt("market_start_date", bigquery.StringFieldType),
		opt("accepting_orders",  bigquery.BooleanFieldType),
		opt("neg_risk",          bigquery.BooleanFieldType),
		req("ingested_at",       bigquery.TimestampFieldType),
	}
}
