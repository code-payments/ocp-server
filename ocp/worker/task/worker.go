package task

import (
	"context"
	"sync"
	"time"

	"github.com/code-payments/ocp-server/database/query"
	"github.com/code-payments/ocp-server/metrics"
	task_data "github.com/code-payments/ocp-server/ocp/data/task"
	"github.com/code-payments/ocp-server/retry"
)

func (p *runtime) worker(runtimeCtx context.Context, interval time.Duration) error {
	var cursor query.Cursor
	delay := interval

	err := retry.Loop(
		func() (err error) {
			time.Sleep(delay)

			provider := runtimeCtx.Value(metrics.ProviderContextKey).(metrics.Provider)
			trace := provider.StartTrace("task_runtime__handle_pending")
			defer trace.End()
			tracedCtx := metrics.NewContext(runtimeCtx, trace)

			items, err := p.data.GetAllReadyTasksByState(
				tracedCtx,
				task_data.StatePending,
				time.Now(),
				query.WithLimit(p.conf.batchSize.Get(runtimeCtx)),
				query.WithCursor(cursor),
			)
			if err == task_data.ErrNotFound {
				cursor = query.EmptyCursor
				return nil
			} else if err != nil {
				cursor = query.EmptyCursor
				return err
			}

			var wg sync.WaitGroup
			for _, item := range items {
				wg.Add(1)

				go func(record *task_data.Record) {
					defer wg.Done()

					err := p.scheduler.ExecuteAndAdvance(tracedCtx, record)
					if err != nil {
						trace.OnError(err)
					}
				}(item)
			}
			wg.Wait()

			if len(items) > 0 {
				cursor = query.ToCursor(items[len(items)-1].Id)
			} else {
				cursor = query.EmptyCursor
			}

			return nil
		},
		retry.NonRetriableErrors(context.Canceled),
	)

	return err
}
