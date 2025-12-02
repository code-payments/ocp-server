package worker

import (
	"context"
	"time"
)

type Runtime interface {
	Start(ctx context.Context, interval time.Duration) error
}
