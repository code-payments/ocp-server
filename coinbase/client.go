package coinbase

import (
	"context"
	"encoding/json"
	"fmt"
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

	// DefaultHost is the host serving the Onramp v1 transaction-status API.
	// Override via Client.Host for staging or to pin to a different region.
	DefaultHost = "api.developer.coinbase.com"

	defaultPageSize = 100
	maxPages        = 50 // hard cap to avoid runaway pagination
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

// GetUserTransactions returns every Onramp transaction Coinbase has recorded
// for the given partnerUserRef under the authenticated CDP project.
// Pagination is handled internally.
func (c *Client) GetUserTransactions(ctx context.Context, partnerUserRef string) ([]*Transaction, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "GetUserTransactions")
	defer tracer.End()

	if partnerUserRef == "" {
		return nil, errors.New("partner user ref is required")
	}

	var all []*Transaction
	var pageKey string
	for range maxPages {
		path := fmt.Sprintf("/onramp/v1/buy/user/%s/transactions", url.PathEscape(partnerUserRef))
		query := url.Values{}
		query.Set("page_size", fmt.Sprintf("%d", defaultPageSize))
		if pageKey != "" {
			query.Set("page_key", pageKey)
		}

		var resp transactionsResponse
		if err := c.get(ctx, path, query, &resp); err != nil {
			tracer.OnError(err)
			return nil, err
		}

		for i := range resp.Transactions {
			all = append(all, resp.Transactions[i].toTransaction())
		}

		if resp.NextPageKey == "" {
			return all, nil
		}
		pageKey = resp.NextPageKey
	}
	err := errors.Errorf("page cap (%d) reached for partner user ref %q", maxPages, partnerUserRef)
	tracer.OnError(err)
	return nil, err
}

// GetUserTransactionByOrderID looks up a single transaction by order ID
// within the partnerUserRef's namespace. Returns ErrTransactionNotFound if
// the order isn't found. The orderID alone is not queryable — callers must
// pair it with the partnerUserRef the order was created under.
func (c *Client) GetUserTransactionByOrderID(ctx context.Context, partnerUserRef, orderID string) (*Transaction, error) {
	tracer := metrics.TraceMethodCall(ctx, metricsStructName, "GetUserTransactionByOrderID")
	defer tracer.End()

	if orderID == "" {
		return nil, errors.New("order id is required")
	}

	txns, err := c.GetUserTransactions(ctx, partnerUserRef)
	if err != nil {
		tracer.OnError(err)
		return nil, err
	}
	for _, t := range txns {
		if t.OrderID == orderID {
			return t, nil
		}
	}
	return nil, ErrTransactionNotFound
}

func (c *Client) get(ctx context.Context, path string, query url.Values, out any) error {
	fullURL := "https://" + c.Host + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}

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
			lastErr = ErrTransactionNotFound
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

// transactionsResponse mirrors the JSON shape of the v1 transactions
// endpoint. Field names match Coinbase's snake_case payload.
type transactionsResponse struct {
	Transactions []transactionPayload `json:"transactions"`
	NextPageKey  string               `json:"next_page_key"`
	TotalCount   string               `json:"total_count"`
}

type transactionPayload struct {
	Status           string         `json:"status"`
	TxHash           string         `json:"tx_hash"`
	WalletAddress    string         `json:"wallet_address"`
	PurchaseAmount   amountPayload  `json:"purchase_amount"`
	PurchaseCurrency string         `json:"purchase_currency"`
	PaymentTotal     *amountPayload `json:"payment_total,omitempty"`
	PartnerUserRef   string         `json:"partner_user_ref"`
	PartnerUserID    string         `json:"partner_user_id"` // legacy field name
	UserID           string         `json:"user_id"`
	TransactionID    string         `json:"transaction_id"` // == orderId UUID
	CreatedAt        time.Time      `json:"created_at"`
}

type amountPayload struct {
	Value    string `json:"value"`
	Currency string `json:"currency"`
}

func (p transactionPayload) toTransaction() *Transaction {
	ref := p.PartnerUserRef
	if ref == "" {
		ref = p.PartnerUserID
	}
	return &Transaction{
		OrderID:         p.TransactionID,
		PartnerUserRef:  ref,
		Status:          TransactionStatus(p.Status),
		TxHash:          p.TxHash,
		WalletAddress:   p.WalletAddress,
		PurchaseAmount:  Amount{Value: p.PurchaseAmount.Value, Currency: p.PurchaseAmount.Currency},
		PurchaseAssetID: p.PurchaseCurrency,
		CreatedAt:       p.CreatedAt,
	}
}
