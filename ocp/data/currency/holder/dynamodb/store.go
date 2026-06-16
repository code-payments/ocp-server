// Package dynamodb implements the holder.Store interface on top of DynamoDB.
//
// Two tables, both keyed per mint (the number of mints is unbounded, so each
// record is its own item — never a map-of-all-mints row):
//
// History table — per-mint time series, one partition per resolution:
//
//	pk = "<mint>#raw"     sk = <nanos>        count        (every sample)
//	pk = "<mint>#hour"    sk = <hour-start>   count, ts    (close per hour)
//	pk = "<mint>#day"     sk = <day-start>    count, ts    (close per day)
//	pk = "<mint>#week"    sk = <week-start>   count, ts    (close per week, Sunday-start)
//	pk = "<mint>#month"   sk = <month-start>  count, ts    (close per month)
//
// Writes fan out to all resolutions in one transaction: the raw point is a
// conditional Put enforcing once-per-timestamp, each rollup a monotonic
// last-write-wins Update holding the bucket close. Rollup items store `ts` (the
// close sample's time, which also drives the guard); raw items omit it because
// their sort key already is the sample time. Reads serve the resolution matching
// the requested interval; point lookups read the raw resolution at or before t.
//
// Live table — latest snapshot per mint, keyed by pk = "<mint>" (no sort key):
//
//	pk = "<mint>"   count, ts
//
// Live writes spread across mints (each mint its own partition); the
// timestamp-monotonic condition keeps the most recent sample. GetAllLiveHolderCounts
// scans this table — it holds one item per mint, so the scan touches only live state.
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/holder"
)

const (
	attrPK    = "pk"
	attrSK    = "sk"
	attrCount = "count"
	attrTS    = "ts"

	resRaw   = "raw"
	resHour  = "hour"
	resDay   = "day"
	resWeek  = "week"
	resMonth = "month"

	// codeConditionalCheckFailed is the DynamoDB cancellation reason code for a
	// transaction item whose ConditionExpression evaluated false.
	codeConditionalCheckFailed = "ConditionalCheckFailed"

	// maxBatchGetItems is DynamoDB's per-call BatchGetItem key limit.
	maxBatchGetItems = 100
)

// rollupResolutions are the coarse resolutions maintained alongside the raw
// points on every historical write.
var rollupResolutions = []string{resHour, resDay, resWeek, resMonth}

type store struct {
	client       *dynamodb.Client
	historyTable string
	liveTable    string
}

// New returns a holder.Store backed by the given DynamoDB tables. Use
// CreateTables to provision them.
func New(client *dynamodb.Client, historyTable, liveTable string) holder.Store {
	return &store{
		client:       client,
		historyTable: historyTable,
		liveTable:    liveTable,
	}
}

// PutHistoricalHolderCount writes the raw sample and refreshes every rollup
// bucket that contains it, atomically. The raw item's attribute_not_exists
// condition enforces once-per-timestamp: a duplicate cancels the transaction and
// returns currency.ErrExists. Rollup guards assume monotonically increasing write
// timestamps per mint (the holder worker's behavior).
func (s *store) PutHistoricalHolderCount(ctx context.Context, record *currency.HolderCountRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}

	count := avNU(record.HolderCount)
	tsVal := skN(record.Time)

	transactItems := []types.TransactWriteItem{
		{Put: &types.Put{
			TableName: aws.String(s.historyTable),
			Item: map[string]types.AttributeValue{
				attrPK:    avS(historyPK(record.Mint, resRaw)),
				attrSK:    skN(record.Time),
				attrCount: count,
			},
			ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
		}},
	}
	for _, res := range rollupResolutions {
		transactItems = append(transactItems, types.TransactWriteItem{
			Update: &types.Update{
				TableName: aws.String(s.historyTable),
				Key: map[string]types.AttributeValue{
					attrPK: avS(historyPK(record.Mint, res)),
					attrSK: skN(bucketStart(record.Time, res)),
				},
				UpdateExpression:    aws.String("SET #c = :c, #ts = :ts"),
				ConditionExpression: aws.String("attribute_not_exists(#pk) OR #ts < :ts"),
				ExpressionAttributeNames: map[string]string{
					"#pk": attrPK,
					"#c":  attrCount,
					"#ts": attrTS,
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":c":  count,
					":ts": tsVal,
				},
			},
		})
	}

	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		var tce *types.TransactionCanceledException
		if errors.As(err, &tce) && len(tce.CancellationReasons) > 0 &&
			aws.ToString(tce.CancellationReasons[0].Code) == codeConditionalCheckFailed {
			return currency.ErrExists
		}
		return err
	}
	return nil
}

func (s *store) GetHolderCountAtTime(ctx context.Context, mint string, t time.Time) (*currency.HolderCountRecord, error) {
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.historyTable),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND %s <= :sk", attrPK, attrSK)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": avS(historyPK(mint, resRaw)),
			":sk": skN(t),
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(1),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Items) == 0 {
		return nil, currency.ErrNotFound
	}
	return historyRecord(mint, out.Items[0])
}

// GetHolderCountsForDay returns each mint's holder count as of the UTC day of t —
// the close of that mint's day rollup bucket. Because a rollup bucket has a
// deterministic key (mint#day, bucketStart), this is a single batched key get
// rather than a per-mint query. Mints with no record on that day are omitted.
func (s *store) GetHolderCountsForDay(ctx context.Context, mints []string, t time.Time) (map[string]*currency.HolderCountRecord, error) {
	bucket := skN(bucketStart(t, resDay))

	// Dedup so a repeated mint isn't fetched twice.
	seen := make(map[string]struct{}, len(mints))
	keys := make([]map[string]types.AttributeValue, 0, len(mints))
	for _, mint := range mints {
		if _, ok := seen[mint]; ok {
			continue
		}
		seen[mint] = struct{}{}
		keys = append(keys, map[string]types.AttributeValue{
			attrPK: avS(historyPK(mint, resDay)),
			attrSK: bucket,
		})
	}

	res := make(map[string]*currency.HolderCountRecord, len(keys))
	for start := 0; start < len(keys); start += maxBatchGetItems {
		end := start + maxBatchGetItems
		if end > len(keys) {
			end = len(keys)
		}

		req := map[string]types.KeysAndAttributes{
			s.historyTable: {Keys: keys[start:end]},
		}
		// Drain UnprocessedKeys (DynamoDB may return a partial batch under load).
		for len(req[s.historyTable].Keys) > 0 {
			out, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
			if err != nil {
				return nil, err
			}
			for _, item := range out.Responses[s.historyTable] {
				mint, err := mintFromDayPK(item)
				if err != nil {
					return nil, err
				}
				rec, err := historyRecord(mint, item)
				if err != nil {
					return nil, err
				}
				res[mint] = rec
			}
			if unprocessed, ok := out.UnprocessedKeys[s.historyTable]; ok && len(unprocessed.Keys) > 0 {
				req = map[string]types.KeysAndAttributes{s.historyTable: unprocessed}
			} else {
				break
			}
		}
	}
	return res, nil
}

func (s *store) GetHolderCountsInRange(ctx context.Context, mint string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.HolderCountRecord, error) {
	if interval > query.IntervalMonth {
		return nil, currency.ErrInvalidInterval
	}
	if start.IsZero() || end.IsZero() {
		return nil, currency.ErrInvalidRange
	}

	actualStart, actualEnd := start, end
	if start.Unix() > end.Unix() {
		actualStart, actualEnd = end, start
	}

	// Honor the requested interval directly by reading the matching stored
	// resolution. The data is pre-aggregated, so no in-app downsampling is needed.
	res := resolutionForInterval(interval)
	items, err := s.queryAll(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.historyTable),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND %s BETWEEN :start AND :end", attrPK, attrSK)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": avS(historyPK(mint, res)),
			// Lower-bound on the bucket containing actualStart so a bucket whose
			// start precedes actualStart is still returned.
			":start": skN(bucketStart(actualStart, res)),
			":end":   skN(actualEnd),
		},
		ScanIndexForward: aws.Bool(ordering != query.Descending),
	})
	if err != nil {
		return nil, err
	}

	records := make([]*currency.HolderCountRecord, 0, len(items))
	for _, item := range items {
		rec, err := historyRecord(mint, item)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	if len(records) == 0 {
		return nil, currency.ErrNotFound
	}
	return records, nil
}

// PutLiveHolderCount upserts the mint's latest holder count, keeping the most
// recent sample. The timestamp-monotonic condition makes a stale (older or equal
// time) write return currency.ErrStaleHolderState.
func (s *store) PutLiveHolderCount(ctx context.Context, record *currency.HolderCountRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.liveTable),
		Item: map[string]types.AttributeValue{
			attrPK:    avS(record.Mint),
			attrCount: avNU(record.HolderCount),
			attrTS:    avN(record.Time.UTC().UnixNano()),
		},
		ConditionExpression: aws.String("attribute_not_exists(#pk) OR #ts < :ts"),
		ExpressionAttributeNames: map[string]string{
			"#pk": attrPK,
			"#ts": attrTS,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":ts": avN(record.Time.UTC().UnixNano()),
		},
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return currency.ErrStaleHolderState
		}
		return err
	}
	return nil
}

func (s *store) GetLiveHolderCount(ctx context.Context, mint string) (*currency.HolderCountRecord, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.liveTable),
		Key:       map[string]types.AttributeValue{attrPK: avS(mint)},
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, currency.ErrNotFound
	}
	return liveRecord(out.Item)
}

func (s *store) GetAllLiveHolderCounts(ctx context.Context) (map[string]*currency.HolderCountRecord, error) {
	res := make(map[string]*currency.HolderCountRecord)
	var startKey map[string]types.AttributeValue
	for {
		out, err := s.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(s.liveTable),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			rec, err := liveRecord(item)
			if err != nil {
				return nil, err
			}
			res[rec.Mint] = rec
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}

	if len(res) == 0 {
		return nil, currency.ErrNotFound
	}
	return res, nil
}

// reset deletes every item from both tables, for tests.
func (s *store) reset() {
	if err := clearTable(context.Background(), s.client, s.historyTable, []string{attrPK, attrSK}); err != nil {
		panic(err)
	}
	if err := clearTable(context.Background(), s.client, s.liveTable, []string{attrPK}); err != nil {
		panic(err)
	}
}

// queryAll runs the query, following LastEvaluatedKey until the result set is
// drained, and returns every matched item.
func (s *store) queryAll(ctx context.Context, input *dynamodb.QueryInput) ([]map[string]types.AttributeValue, error) {
	var items []map[string]types.AttributeValue
	for {
		out, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		items = append(items, out.Items...)
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
	return items, nil
}

// historyRecord builds a holder count record from a history item. The mint is
// the query parameter (it is encoded in the pk alongside the resolution, so it
// is not duplicated onto the item).
func historyRecord(mint string, item map[string]types.AttributeValue) (*currency.HolderCountRecord, error) {
	count, err := parseNU(item[attrCount])
	if err != nil {
		return nil, err
	}
	ts, err := itemTime(item)
	if err != nil {
		return nil, err
	}
	return &currency.HolderCountRecord{
		Mint:        mint,
		HolderCount: count,
		Time:        ts,
	}, nil
}

func liveRecord(item map[string]types.AttributeValue) (*currency.HolderCountRecord, error) {
	count, err := parseNU(item[attrCount])
	if err != nil {
		return nil, err
	}
	nanos, err := parseN(item[attrTS])
	if err != nil {
		return nil, err
	}
	return &currency.HolderCountRecord{
		Mint:        asS(item[attrPK]),
		HolderCount: count,
		Time:        time.Unix(0, nanos).UTC(),
	}, nil
}

// itemTime returns the point's timestamp: the close sample time from `ts` for
// rollup items, or the sample time from the sort key for raw items (which omit
// `ts`).
func itemTime(item map[string]types.AttributeValue) (time.Time, error) {
	av, ok := item[attrTS]
	if !ok {
		av = item[attrSK]
	}
	nanos, err := parseN(av)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, nanos).UTC(), nil
}

// resolutionForInterval maps a requested interval to the stored resolution that
// serves it. Sub-hour intervals are served from raw, the finest stored
// resolution. The caller chooses an interval appropriate to the range; this
// store honors it directly rather than re-deriving one.
func resolutionForInterval(interval query.Interval) string {
	switch interval {
	case query.IntervalHour:
		return resHour
	case query.IntervalDay:
		return resDay
	case query.IntervalWeek:
		return resWeek
	case query.IntervalMonth:
		return resMonth
	default: // raw, second, minute
		return resRaw
	}
}

// bucketStart truncates t to the start of its bucket for the given resolution,
// in UTC. Weeks start on Sunday.
func bucketStart(t time.Time, res string) time.Time {
	t = t.UTC()
	switch res {
	case resHour:
		return t.Truncate(time.Hour)
	case resDay:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case resWeek:
		day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		return day.AddDate(0, 0, -int(day.Weekday())) // time.Weekday: Sunday=0
	case resMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	default: // resRaw
		return t
	}
}

func historyPK(mint, res string) string { return mint + "#" + res }

// mintFromDayPK recovers the mint from a day-rollup item's pk ("<mint>#day").
// Mints are base58, which never contains '#', so the resolution suffix is
// unambiguous.
func mintFromDayPK(item map[string]types.AttributeValue) (string, error) {
	pk := asS(item[attrPK])
	suffix := "#" + resDay
	if !strings.HasSuffix(pk, suffix) {
		return "", fmt.Errorf("unexpected day-rollup pk %q", pk)
	}
	return strings.TrimSuffix(pk, suffix), nil
}

// skN encodes t's unix-nanos as the numeric sort key, which sorts in
// chronological order.
func skN(t time.Time) types.AttributeValue { return avN(t.UTC().UnixNano()) }

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avN(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}
func avNU(v uint64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
}

func asS(av types.AttributeValue) string {
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return s.Value
	}
	return ""
}

func parseN(av types.AttributeValue) (int64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseInt(n.Value, 10, 64)
}

func parseNU(av types.AttributeValue) (uint64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseUint(n.Value, 10, 64)
}
