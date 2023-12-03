package pgxscan

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type ctxKey struct{}

var txCtxKey = &ctxKey{}

func contextWithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txCtxKey, tx)
}

func txFromContext(ctx context.Context) pgx.Tx {
	v, _ := ctx.Value(txCtxKey).(pgx.Tx)
	return v
}
