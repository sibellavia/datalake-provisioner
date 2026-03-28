package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/movincloud/datalake-provisioner/internal/domain"
	"github.com/movincloud/datalake-provisioner/internal/observability"
)

const defaultAdvisoryLockKey int64 = 44831101

type Service interface {
	ClaimNextRunnableOperation(ctx context.Context) (domain.Operation, bool, error)
	ResetStaleRunningOperations(ctx context.Context, staleBefore time.Time) (int64, error)
	ExecuteOperation(ctx context.Context, op domain.Operation) error
	RequeueOperation(ctx context.Context, op domain.Operation, err error, nextAttemptAt time.Time) error
	MarkOperationExecutionFailed(ctx context.Context, op domain.Operation, err error) error
}

type Runner struct {
	DB           *pgxpool.Pool
	Service      Service
	PollInterval time.Duration
	StaleAfter   time.Duration
	MaxAttempts  int
	LockKey      int64
}

func (r *Runner) Run(ctx context.Context) {
	observability.SetWorkerLeader(false)

	if r == nil || r.DB == nil || r.Service == nil {
		slog.WarnContext(ctx, "worker disabled: missing db or service", "component", "worker")
		return
	}
	if r.PollInterval <= 0 {
		r.PollInterval = 2 * time.Second
	}
	if r.StaleAfter <= 0 {
		r.StaleAfter = 2 * time.Minute
	}
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = 3
	}
	if r.LockKey == 0 {
		r.LockKey = defaultAdvisoryLockKey
	}

	var leaderConn *pgxpool.Conn
	for {
		if ctx.Err() != nil {
			r.releaseLeadership(ctx, leaderConn)
			return
		}

		if leaderConn == nil {
			conn, err := r.tryAcquireLeadership(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "worker leadership acquire failed", "component", "worker", "error.message", err.Error())
				r.sleep(ctx, r.PollInterval)
				continue
			}
			if conn == nil {
				r.sleep(ctx, r.PollInterval)
				continue
			}
			leaderConn = conn
			observability.SetWorkerLeader(true)
			slog.InfoContext(ctx, "worker leadership acquired", "component", "worker")
		}

		if err := r.ensureLeadership(ctx, leaderConn); err != nil {
			slog.ErrorContext(ctx, "worker leadership lost", "component", "worker", "error.message", err.Error())
			r.releaseLeadership(context.Background(), leaderConn)
			leaderConn = nil
			r.sleep(ctx, r.PollInterval)
			continue
		}

		staleBefore := time.Now().UTC().Add(-r.StaleAfter)
		if count, err := r.Service.ResetStaleRunningOperations(ctx, staleBefore); err != nil {
			slog.ErrorContext(ctx, "worker stale reset failed", "component", "worker", "error.message", err.Error())
			r.sleep(ctx, r.PollInterval)
			continue
		} else if count > 0 {
			observability.AddWorkerStaleResets(count)
			slog.InfoContext(ctx, "worker reset stale running operations", "component", "worker", "count", count)
		}

		op, ok, err := r.Service.ClaimNextRunnableOperation(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "worker claim failed", "component", "worker", "error.message", err.Error())
			r.sleep(ctx, r.PollInterval)
			continue
		}
		if !ok {
			r.sleep(ctx, r.PollInterval)
			continue
		}

		observability.ObserveWorkerClaim(op.OperationType)
		slog.InfoContext(ctx, "worker claimed operation",
			"component", "worker",
			"operation.id", op.OperationID,
			"operation.type", op.OperationType,
			"tenant.id", op.TenantID,
			"lake.id", op.LakeID,
			"bucket.id", op.BucketID,
			"attempt", op.AttemptCount,
		)
		executionStartedAt := time.Now()
		if err := r.Service.ExecuteOperation(ctx, op); err != nil {
			if op.AttemptCount >= r.MaxAttempts {
				observability.ObserveWorkerExecution(op.OperationType, "failed", time.Since(executionStartedAt))
				slog.ErrorContext(ctx, "worker failing operation permanently",
					"component", "worker",
					"operation.id", op.OperationID,
					"operation.type", op.OperationType,
					"attempt", op.AttemptCount,
					"error.message", err.Error(),
				)
				if markErr := r.Service.MarkOperationExecutionFailed(ctx, op, err); markErr != nil {
					slog.ErrorContext(ctx, "worker failed to mark final failure",
						"component", "worker",
						"operation.id", op.OperationID,
						"error.message", markErr.Error(),
					)
				}
				continue
			}

			observability.ObserveWorkerExecution(op.OperationType, "requeued", time.Since(executionStartedAt))
			observability.ObserveWorkerRequeue(op.OperationType)
			nextAttemptAt := time.Now().UTC().Add(retryDelay(op.AttemptCount))
			slog.WarnContext(ctx, "worker requeueing operation",
				"component", "worker",
				"operation.id", op.OperationID,
				"operation.type", op.OperationType,
				"attempt", op.AttemptCount,
				"next_attempt_at", nextAttemptAt.Format(time.RFC3339),
				"error.message", err.Error(),
			)
			if requeueErr := r.Service.RequeueOperation(ctx, op, err, nextAttemptAt); requeueErr != nil {
				slog.ErrorContext(ctx, "worker failed to requeue operation",
					"component", "worker",
					"operation.id", op.OperationID,
					"error.message", requeueErr.Error(),
				)
				if markErr := r.Service.MarkOperationExecutionFailed(ctx, op, fmt.Errorf("requeue failed after execution error %v: %w", requeueErr, err)); markErr != nil {
					slog.ErrorContext(ctx, "worker failed to mark fallback failure",
						"component", "worker",
						"operation.id", op.OperationID,
						"error.message", markErr.Error(),
					)
				}
			}
			continue
		}

		observability.ObserveWorkerExecution(op.OperationType, "success", time.Since(executionStartedAt))
	}
}

func (r *Runner) tryAcquireLeadership(ctx context.Context) (*pgxpool.Conn, error) {
	conn, err := r.DB.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, r.LockKey).Scan(&ok); err != nil {
		conn.Release()
		return nil, err
	}
	if !ok {
		conn.Release()
		return nil, nil
	}
	return conn, nil
}

func (r *Runner) ensureLeadership(ctx context.Context, conn *pgxpool.Conn) error {
	if conn == nil {
		return fmt.Errorf("no leadership connection")
	}
	var n int
	return conn.QueryRow(ctx, `SELECT 1`).Scan(&n)
}

func (r *Runner) releaseLeadership(ctx context.Context, conn *pgxpool.Conn) {
	if conn == nil {
		observability.SetWorkerLeader(false)
		return
	}
	_, _ = conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, r.LockKey)
	conn.Release()
	observability.SetWorkerLeader(false)
}

func (r *Runner) sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func retryDelay(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 10 * time.Second
	case 2:
		return 30 * time.Second
	default:
		return 60 * time.Second
	}
}
