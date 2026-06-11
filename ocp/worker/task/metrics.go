package task

import (
	"context"
	"time"

	"github.com/code-payments/ocp-server/metrics"
	task_data "github.com/code-payments/ocp-server/ocp/data/task"
)

const (
	taskCountEventName = "TaskCountPollingCheck"
)

func (p *runtime) metricsGaugeWorker(ctx context.Context) error {
	delay := time.Second

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			start := time.Now()

			for _, state := range []task_data.State{
				task_data.StatePending,
				task_data.StateFailed,
			} {
				count, err := p.data.GetTaskCountByState(ctx, state)
				if err != nil {
					continue
				}
				recordTaskCountEvent(ctx, state, count)
			}

			delay = time.Second - time.Since(start)
		}
	}
}

func recordTaskCountEvent(ctx context.Context, state task_data.State, count uint64) {
	metrics.RecordEvent(ctx, taskCountEventName, map[string]interface{}{
		"count": count,
		"state": state.String(),
	})
}
