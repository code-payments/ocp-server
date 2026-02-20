package nonce

import (
	"context"
	"time"
)

const (
	nonceCountCheckEventName = "NonceCountPollingCheck"
)

func (p *runtime) metricsGaugeWorker(ctx context.Context) error {
	delay := time.Second

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// todo: define valuable metrics
		}
	}
}
