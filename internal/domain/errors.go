package domain

import "errors"

var ErrIdempotencyMismatch = errors.New("idempotency key reused with different request")
