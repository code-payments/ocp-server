package coinbase

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"

	"github.com/code-payments/ocp-server/metrics"
	"github.com/code-payments/ocp-server/retry"
	"github.com/code-payments/ocp-server/retry/backoff"
)

const (
	metricsStructName = "coinbase.client"

	// DefaultHost is the host serving the Onramp API. Override via
	// Client.Host for staging or to pin to a different region.
	DefaultHost = "api.cdp.coinbase.com"
)

// Client talks to the Coinbase Developer Platform Onramp API. Construct via
// NewClient and share across goroutines — http.Client and the Authenticator
// are both safe for concurrent use.
type Client struct {
	httpClient *http.Client
	auth       *Authenticator
	retrier    retry.Retrier

	// Host is the API host (no scheme, no trailing slash). Defaults to
	// DefaultHost. The same host is used for the JWT `uri` claim, so changing
	// it must match the actual server.
	Host string
}

func NewClient(auth *Authenticator) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		auth: auth,
		retrier: retry.NewRetrier(
			retry.NonRetriableErrors(context.Canceled),
			retry.Limit(3),
			retry.BackoffWithJitter(backoff.BinaryExponential(time.Second), 10*time.Second, 0.1),
		),
		Host: DefaultHost,
	}
}

// GetOrder returns the Onramp order with the given order ID. Returns
// ErrOrderNotFound if the order isn't present in the authenticated CDP
// project.
func (c *Client) GetOrder(ctx context.Context, orderID string) (*Order, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "GetOrder")
	defer tracer.End()

	if orderID == "" {
		return nil, errors.New("order id is required")
	}

	path := "/platform/v2/onramp/orders/" + url.PathEscape(orderID)

	var resp orderResponse
	if err := c.get(ctx, path, &resp); err != nil {
		tracer.OnError(err)
		return nil, err
	}
	return resp.Order.toOrder(), nil
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	fullURL := "https://" + c.Host + path

	var lastErr error
	_, err := c.retrier.Retry(func() error {
		// JWTs are bound to METHOD+HOST+PATH and expire in 2 minutes, so
		// mint a fresh one per attempt rather than per call.
		jwt, err := c.auth.JWT(http.MethodGet, c.Host, path)
		if err != nil {
			lastErr = errors.Wrap(err, "error generating jwt")
			return lastErr
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, http.NoBody)
		if err != nil {
			lastErr = errors.Wrap(err, "error building request")
			return lastErr
		}
		req.Header.Set("Authorization", "Bearer "+jwt)
		req.Header.Set("Accept", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = errors.Wrap(err, "error sending request")
			return lastErr
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			lastErr = ErrOrderNotFound
			return nil // do not retry
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastErr = errors.Errorf("coinbase api returned status %d", resp.StatusCode)
			// Retry 5xx; 4xx is the caller's problem.
			if resp.StatusCode >= 500 {
				return lastErr
			}
			return nil
		}

		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			lastErr = errors.Wrap(err, "error decoding response")
			return lastErr
		}
		lastErr = nil
		return nil
	})
	if err != nil {
		return err
	}
	return lastErr
}

// orderResponse mirrors the JSON envelope of the v2 order endpoint.
type orderResponse struct {
	Order orderPayload `json:"order"`
}

type orderPayload struct {
	OrderID            string       `json:"orderId"`
	Status             string       `json:"status"`
	PaymentTotal       string       `json:"paymentTotal"`
	PaymentSubtotal    string       `json:"paymentSubtotal"`
	PaymentCurrency    string       `json:"paymentCurrency"`
	PaymentMethod      string       `json:"paymentMethod"`
	PurchaseAmount     string       `json:"purchaseAmount"`
	PurchaseCurrency   string       `json:"purchaseCurrency"`
	Fees               []feePayload `json:"fees"`
	ExchangeRate       string       `json:"exchangeRate"`
	DestinationAddress string       `json:"destinationAddress"`
	DestinationNetwork string       `json:"destinationNetwork"`
	TxHash             string       `json:"txHash"`
	PartnerUserRef     string       `json:"partnerUserRef"`
	CreatedAt          time.Time    `json:"createdAt"`
	UpdatedAt          time.Time    `json:"updatedAt"`
}

type feePayload struct {
	Type     string `json:"type"`
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (p orderPayload) toOrder() *Order {
	fees := make([]Fee, 0, len(p.Fees))
	for _, f := range p.Fees {
		fees = append(fees, Fee{
			Type:   FeeType(f.Type),
			Amount: Amount{Value: f.Amount, Currency: f.Currency},
		})
	}
	return &Order{
		OrderID:            p.OrderID,
		Status:             OrderStatus(p.Status),
		PaymentTotal:       Amount{Value: p.PaymentTotal, Currency: p.PaymentCurrency},
		PaymentSubtotal:    Amount{Value: p.PaymentSubtotal, Currency: p.PaymentCurrency},
		PaymentMethod:      PaymentMethod(p.PaymentMethod),
		PurchaseAmount:     Amount{Value: p.PurchaseAmount, Currency: p.PurchaseCurrency},
		Fees:               fees,
		ExchangeRate:       p.ExchangeRate,
		DestinationAddress: p.DestinationAddress,
		DestinationNetwork: p.DestinationNetwork,
		TxHash:             p.TxHash,
		PartnerUserRef:     p.PartnerUserRef,
		CreatedAt:          p.CreatedAt,
		UpdatedAt:          p.UpdatedAt,
	}
}
