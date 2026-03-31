package exchangerateapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/currency"
	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/retry"
	"github.com/code-payments/ocp-server/retry/backoff"
)

const (
	metricsStructName = "currency.exchangerateapi.client"
)

const (
	baseUrl             = "https://v6.exchangerate-api.com/v6"
	latestUrlFormat     = baseUrl + "/%s/latest/%s"
	historicalUrlFormat = baseUrl + "/%s/history/%s/%d/%d/%d"
)

const (
	resultSuccess        = "success"
	errorUnsupportedCode = "unsupported-code"
)

// API Documentation: https://www.exchangerate-api.com/docs/overview
type client struct {
	apiKey     string
	httpClient *http.Client
	retrier    retry.Retrier
}

func NewClient(apiKey string) currency.Client {
	return &client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		retrier: retry.NewRetrier(
			retry.NonRetriableErrors(context.Canceled),
			retry.Limit(3),
			retry.BackoffWithJitter(backoff.BinaryExponential(time.Second), 10*time.Second, 0.1),
		),
	}
}

// GetCurrentRates implements currency.Client.GetCurrentRates
func (c *client) GetCurrentRates(ctx context.Context, base string) (*currency.ExchangeData, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "GetCurrentRates")
	defer tracer.End()

	var resp response
	url := fmt.Sprintf(latestUrlFormat, c.apiKey, strings.ToUpper(base))
	err := c.submitRequest(ctx, url, &resp)
	if err != nil {
		tracer.OnError(err)
		return nil, err
	}

	err = checkCustomError(resp)
	if err != nil {
		tracer.OnError(err)
		return nil, err
	}

	return resp.toExchangeData()
}

// GetHistoricalRates implements currency.Client.GetHistoricalRates
func (c *client) GetHistoricalRates(ctx context.Context, base string, timestamp time.Time) (*currency.ExchangeData, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "GetHistoricalRates")
	defer tracer.End()

	var resp response
	url := fmt.Sprintf(historicalUrlFormat, c.apiKey, strings.ToUpper(base), timestamp.Year(), int(timestamp.Month()), timestamp.Day())
	err := c.submitRequest(ctx, url, &resp)
	if err != nil {
		tracer.OnError(err)
		return nil, err
	}

	err = checkCustomError(resp)
	if err != nil {
		tracer.OnError(err)
		return nil, err
	}

	return resp.toExchangeData()
}

func (c *client) submitRequest(ctx context.Context, url string, resp interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	var httpResp *http.Response
	_, err = c.retrier.Retry(
		func() error {
			httpResp, err = c.httpClient.Do(req)
			return err
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to make request")
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return errors.Errorf("received non-200 status code: %d", httpResp.StatusCode)
	}

	err = json.NewDecoder(httpResp.Body).Decode(resp)
	if err != nil {
		return errors.Wrap(err, "failed to decode response")
	}

	return nil
}

func checkCustomError(resp response) error {
	if resp.Result == resultSuccess {
		return nil
	}

	switch resp.ErrorType {
	case errorUnsupportedCode:
		return currency.ErrInvalidBase
	case "":
		return errors.New("unknown error from exchangerate-api without error type")
	default:
		return errors.Errorf("exchangerate-api error: %s", resp.ErrorType)
	}
}

type response struct {
	Result        string             `json:"result"`
	ErrorType     string             `json:"error-type"`
	BaseCode      string             `json:"base_code"`
	TimeLastUpdate int64             `json:"time_last_update_unix"`
	ConversionRates map[string]float64 `json:"conversion_rates"`
}

func (r response) toExchangeData() (*currency.ExchangeData, error) {
	if r.Result != resultSuccess {
		return nil, errors.New("cannot convert a failed response")
	}

	rates := make(map[string]float64)
	for symbol, rate := range r.ConversionRates {
		rates[strings.ToLower(symbol)] = rate
	}
	rates = excludeUnsupported(rates)

	return &currency.ExchangeData{
		Base:      strings.ToLower(r.BaseCode),
		Rates:     rates,
		Timestamp: time.Unix(r.TimeLastUpdate, 0),
	}, nil
}

func excludeUnsupported(data map[string]float64) map[string]float64 {
	res := make(map[string]float64, 0)

	for symbol, rate := range data {
		if len(symbol) == 3 {
			res[symbol] = rate
		}
	}

	return res
}
