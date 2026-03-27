package domain

import "errors"

var (
	ErrIdempotencyMismatch = errors.New("idempotency key reused with different request")
	ErrConflict            = errors.New("conflicting operation already exists")
	ErrInvalidState        = errors.New("operation not allowed in current state")
)
