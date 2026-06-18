// Package dynamodb implements the exchange.Store interface on top of DynamoDB.
//
// Data model (single table, no secondary indexes). Items are grouped into one
// partition per time resolution:
//
//	pk = "rates#raw"     sk = <nanos>          rates             (every sample)
//	pk = "rates#hour"    sk = <hour-start>     ts, rates         (close per hour)
//	pk = "rates#day"     sk = <day-start>      ts, rates         (close per day)
//	pk = "rates#week"    sk = <week-start>     ts, rates         (close per week, Sunday-start)
//	pk = "rates#month"   sk = <month-start>    ts, rates         (close per month)
//
// Every item carries the full symbol->rate map for that instant; rollup items
// hold the map from the most recent sample in their bucket (the "close"). The
// sort key is the bucket-start time as unix-nanos (a Number), which sorts in
// chronological order. Rollup items also store `ts`, the close
// sample's actual time, which both drives the monotonic last-write-wins guard
// and is the timestamp returned for the point. Raw items omit `ts` because their
// sort key already is the sample time.
//
// Writes fan out to all resolutions in a single transaction: the raw point is a
// conditional Put that enforces the once-per-timestamp contract, and each rollup
// is a monotonic last-write-wins Update. Reads serve the resolution matching the
// requested interval (see resolutionForInterval): the caller chooses an interval
// appropriate to the range and the store honors it directly. Point lookups
// always read the raw resolution.
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/ocp/data/currency"
	"github.com/code-payments/ocp-server/ocp/data/currency/exchange"
)

const (
	attrPK    = "pk"
	attrSK    = "sk"
	attrTS    = "ts"
	attrRates = "rates"

	resRaw   = "raw"
	resHour  = "hour"
	resDay   = "day"
	resWeek  = "week"
	resMonth = "month"

	partitionPrefix = "rates#"
	rawPK           = partitionPrefix + resRaw

	// codeConditionalCheckFailed is the DynamoDB cancellation reason code for a
	// transaction item whose ConditionExpression evaluated false.
	codeConditionalCheckFailed = "ConditionalCheckFailed"
)

// rollupResolutions are the coarse resolutions maintained alongside the raw
// points on every write.
var rollupResolutions = []string{resHour, resDay, resWeek, resMonth}

type store struct {
	client *dynamodb.Client
	table  string
}

// New returns an exchange.Store backed by the given DynamoDB table.
// Use CreateTables to provision it.
func New(client *dynamodb.Client, table string) exchange.Store {
	return &store{
		client: client,
		table:  table,
	}
}

// PutExchangeRates writes the raw sample and refreshes every rollup bucket that
// contains it, atomically, in a single transaction. The raw item's
// attribute_not_exists condition enforces the once-per-timestamp contract: a
// duplicate timestamp cancels the transaction and returns currency.ErrExists.
//
// The rollup updates carry a monotonic guard (only advance a bucket to a newer
// sample). Because a transaction is all-or-nothing, this assumes monotonically
// increasing write timestamps — the case for the periodic rate worker. An
// out-of-order (older) sample would fail a rollup guard and cancel the whole
// transaction, surfacing as an error rather than partially applying.
func (s *store) PutExchangeRates(ctx context.Context, record *currency.MultiRateRecord) error {
	if len(record.Rates) == 0 {
		return nil
	}

	rates := make(map[string]types.AttributeValue, len(record.Rates))
	for symbol, rate := range record.Rates {
		rates[symbol] = avF(rate)
	}
	ratesAttr := &types.AttributeValueMemberM{Value: rates}
	ts := avN(record.Time.UTC().UnixNano())

	// The raw item is first so that a duplicate timestamp is identifiable as the
	// zeroth cancellation reason.
	transactItems := []types.TransactWriteItem{
		{Put: &types.Put{
			TableName: aws.String(s.table),
			Item: map[string]types.AttributeValue{
				attrPK:    avS(rawPK),
				attrSK:    skN(record.Time),
				attrRates: ratesAttr,
			},
			ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrPK)),
		}},
	}
	for _, res := range rollupResolutions {
		transactItems = append(transactItems, types.TransactWriteItem{
			Update: &types.Update{
				TableName: aws.String(s.table),
				Key: map[string]types.AttributeValue{
					attrPK: avS(partition(res)),
					attrSK: skN(bucketStart(record.Time, res)),
				},
				UpdateExpression:    aws.String("SET #r = :rates, #ts = :ts"),
				ConditionExpression: aws.String("attribute_not_exists(#pk) OR #ts < :ts"),
				ExpressionAttributeNames: map[string]string{
					"#pk": attrPK,
					"#r":  attrRates,
					"#ts": attrTS,
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":rates": ratesAttr,
					":ts":    ts,
				},
			},
		})
	}

	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		// A cancellation whose raw item (index 0) failed its condition means a
		// record already exists for this timestamp.
		var tce *types.TransactionCanceledException
		if errors.As(err, &tce) && len(tce.CancellationReasons) > 0 &&
			aws.ToString(tce.CancellationReasons[0].Code) == codeConditionalCheckFailed {
			return currency.ErrExists
		}
		return err
	}
	return nil
}

func (s *store) GetExchangeRate(ctx context.Context, symbol string, t time.Time) (*currency.ExchangeRateRecord, error) {
	// Only this symbol's rate is needed, so project it out of the full rate map.
	projection, names := symbolProjection(symbol)
	item, err := s.latestItemAtOrBefore(ctx, t, projection, names)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, currency.ErrNotFound
	}

	rate, ok, err := rateForSymbol(item, symbol)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, currency.ErrNotFound
	}
	return rate, nil
}

func (s *store) GetAllExchangeRates(ctx context.Context, t time.Time) (*currency.MultiRateRecord, error) {
	// The whole rate map is returned, so no projection.
	item, err := s.latestItemAtOrBefore(ctx, t, "", nil)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, currency.ErrNotFound
	}

	ts, err := itemTime(item)
	if err != nil {
		return nil, err
	}
	res := &currency.MultiRateRecord{
		Time:  ts,
		Rates: make(map[string]float64),
	}
	for symbol, av := range asM(item[attrRates]) {
		rate, err := parseF(av)
		if err != nil {
			return nil, err
		}
		res.Rates[symbol] = rate
	}
	return res, nil
}

func (s *store) GetExchangeRatesInRange(ctx context.Context, symbol string, interval query.Interval, start time.Time, end time.Time, ordering query.Ordering) ([]*currency.ExchangeRateRecord, error) {
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
	projection, names := symbolProjection(symbol)
	items, err := s.queryAll(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND %s BETWEEN :start AND :end", attrPK, attrSK)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": avS(partition(res)),
			// Lower-bound on the bucket containing actualStart so a bucket whose
			// start precedes actualStart is still returned.
			":start": skN(bucketStart(actualStart, res)),
			":end":   skN(actualEnd),
		},
		ProjectionExpression:     aws.String(projection),
		ExpressionAttributeNames: names,
		ScanIndexForward:         aws.Bool(ordering != query.Descending),
	})
	if err != nil {
		return nil, err
	}

	records := make([]*currency.ExchangeRateRecord, 0, len(items))
	for _, item := range items {
		rate, ok, err := rateForSymbol(item, symbol)
		if err != nil {
			return nil, err
		}
		if ok {
			records = append(records, rate)
		}
	}

	if len(records) == 0 {
		return nil, currency.ErrNotFound
	}
	return records, nil
}

// reset deletes every item from the table, for tests.
func (s *store) reset() {
	if err := clearTable(context.Background(), s.client, s.table); err != nil {
		panic(err)
	}
}

// latestItemAtOrBefore returns the most recent raw item at or before t, or nil
// if none exists. A non-empty projection (with its attribute-name aliases)
// limits which attributes are returned.
func (s *store) latestItemAtOrBefore(ctx context.Context, t time.Time, projection string, names map[string]string) (map[string]types.AttributeValue, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND %s <= :sk", attrPK, attrSK)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": avS(rawPK),
			":sk": skN(t),
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(1),
	}
	if projection != "" {
		input.ProjectionExpression = aws.String(projection)
		input.ExpressionAttributeNames = names
	}

	out, err := s.client.Query(ctx, input)
	if err != nil {
		return nil, err
	}
	if len(out.Items) == 0 {
		return nil, nil
	}
	return out.Items[0], nil
}

// symbolProjection builds a ProjectionExpression (and its attribute-name
// aliases) that fetches only the given symbol's rate from the rates map, plus
// ts and optionally date — avoiding deserialization of the full rate map when
// only one symbol is needed.
func symbolProjection(symbol string) (string, map[string]string) {
	// #sk is projected so the sample time can be recovered from the sort key for
	// raw items, which omit #ts.
	names := map[string]string{"#sk": attrSK, "#ts": attrTS, "#r": attrRates, "#s": symbol}
	return "#sk, #ts, #r.#s", names
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

// resolutionForInterval maps a requested interval to the stored resolution that
// serves it. Sub-hour intervals are served from raw, the finest stored
// resolution. The caller is responsible for choosing an interval appropriate to
// the range; this store honors it directly rather than re-deriving one.
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

// rateForSymbol builds the per-symbol record from an item's rate map, reporting
// whether the symbol is present.
func rateForSymbol(item map[string]types.AttributeValue, symbol string) (*currency.ExchangeRateRecord, bool, error) {
	av, ok := asM(item[attrRates])[symbol]
	if !ok {
		return nil, false, nil
	}
	rate, err := parseF(av)
	if err != nil {
		return nil, false, err
	}
	ts, err := itemTime(item)
	if err != nil {
		return nil, false, err
	}
	return &currency.ExchangeRateRecord{
		Time:   ts,
		Symbol: symbol,
		Rate:   rate,
	}, true, nil
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

func partition(res string) string { return partitionPrefix + res }

// skN encodes t's unix-nanos as the numeric sort key, which sorts in
// chronological order.
func skN(t time.Time) types.AttributeValue { return avN(t.UTC().UnixNano()) }

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func avN(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}
func avF(v float64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatFloat(v, 'g', -1, 64)}
}

func asM(av types.AttributeValue) map[string]types.AttributeValue {
	if m, ok := av.(*types.AttributeValueMemberM); ok {
		return m.Value
	}
	return nil
}

func parseN(av types.AttributeValue) (int64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseInt(n.Value, 10, 64)
}

func parseF(av types.AttributeValue) (float64, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("expected number attribute, got %T", av)
	}
	return strconv.ParseFloat(n.Value, 64)
}
