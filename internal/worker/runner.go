package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/movincloud/datalake-provisioner/internal/domain"
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
	if r == nil || r.DB == nil || r.Service == nil {
		log.Printf("worker disabled: missing db or service")
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
				log.Printf("worker leadership acquire failed: %v", err)
				r.sleep(ctx, r.PollInterval)
				continue
			}
			if conn == nil {
				r.sleep(ctx, r.PollInterval)
				continue
			}
			leaderConn = conn
			log.Printf("worker leadership acquired")
		}

		if err := r.ensureLeadership(ctx, leaderConn); err != nil {
			log.Printf("worker leadership lost: %v", err)
			r.releaseLeadership(context.Background(), leaderConn)
			leaderConn = nil
			r.sleep(ctx, r.PollInterval)
			continue
		}

		staleBefore := time.Now().UTC().Add(-r.StaleAfter)
		if count, err := r.Service.ResetStaleRunningOperations(ctx, staleBefore); err != nil {
			log.Printf("worker stale reset failed: %v", err)
			r.sleep(ctx, r.PollInterval)
			continue
		} else if count > 0 {
			log.Printf("worker reset %d stale running operation(s)", count)
		}

		op, ok, err := r.Service.ClaimNextRunnableOperation(ctx)
		if err != nil {
			log.Printf("worker claim failed: %v", err)
			r.sleep(ctx, r.PollInterval)
			continue
		}
		if !ok {
			r.sleep(ctx, r.PollInterval)
			continue
		}

		log.Printf("worker claimed op=%s type=%s tenant=%s lake=%s bucket=%s attempt=%d", op.OperationID, op.OperationType, op.TenantID, op.LakeID, op.BucketID, op.AttemptCount)
		if err := r.Service.ExecuteOperation(ctx, op); err != nil {
			if op.AttemptCount >= r.MaxAttempts {
				log.Printf("worker failing op=%s permanently after %d attempt(s): %v", op.OperationID, op.AttemptCount, err)
				if markErr := r.Service.MarkOperationExecutionFailed(ctx, op, err); markErr != nil {
					log.Printf("worker failed to mark final failure op=%s: %v", op.OperationID, markErr)
				}
				continue
			}

			nextAttemptAt := time.Now().UTC().Add(retryDelay(op.AttemptCount))
			log.Printf("worker requeueing op=%s attempt=%d nextAttemptAt=%s err=%v", op.OperationID, op.AttemptCount, nextAttemptAt.Format(time.RFC3339), err)
			if requeueErr := r.Service.RequeueOperation(ctx, op, err, nextAttemptAt); requeueErr != nil {
				log.Printf("worker failed to requeue op=%s: %v", op.OperationID, requeueErr)
				if markErr := r.Service.MarkOperationExecutionFailed(ctx, op, fmt.Errorf("requeue failed after execution error %v: %w", requeueErr, err)); markErr != nil {
					log.Printf("worker failed to mark fallback failure op=%s: %v", op.OperationID, markErr)
				}
			}
		}
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
		return
	}
	_, _ = conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, r.LockKey)
	conn.Release()
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
