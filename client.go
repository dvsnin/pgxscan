package pgxscan

import (
	"context"
	"fmt"
	"time"

	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ Client = (*client)(nil)

type Client interface {
	Get(ctx context.Context, dest interface{}, sql string, args ...interface{}) error
	Select(ctx context.Context, dest interface{}, sql string, args ...interface{}) error
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	InTx(ctx context.Context, isoLevel pgx.TxIsoLevel, f func(ctx context.Context) error) error
	Ping(ctx context.Context) error
	Close()
}

func New(ctx context.Context, config Config) (Client, error) {
	dsnPool := dbDSN(config)
	pgxConfig, err := pgxpool.ParseConfig(dsnPool)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	if config.EnableBeforeAcquirePing {
		pgxConfig.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
			return conn.Ping(ctx) == nil
		}
	}

	pool, err := pgxpool.NewWithConfig(ctx, pgxConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping to database connection: %w", err)
	}

	timeout := time.Second * 5
	if config.QueryTimeout > 0 {
		timeout = config.QueryTimeout
	}

	dbScan, err := pgxscan.NewDBScanAPI(dbscan.WithAllowUnknownColumns(config.AllowUnknownColumns))
	if err != nil {
		return nil, fmt.Errorf("pgxscan.NewDBScanAPI: %w", err)
	}

	pgxscanApi, err := pgxscan.NewAPI(dbScan)
	if err != nil {
		return nil, fmt.Errorf("pgxscan.NewAPI: %w", err)
	}

	return &client{
		dbPool:       pool,
		queryTimeout: timeout,
		scanApi:      pgxscanApi,
	}, nil
}

// Client for db
type client struct {
	dbPool       *pgxpool.Pool
	queryTimeout time.Duration
	scanApi      *pgxscan.API
}

func (c *client) Get(ctx context.Context, dest interface{}, sql string, args ...interface{}) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	if err := c.getInTx(ctxWithTimeout, dest, sql, args...); err != nil {
		if pgxscan.NotFound(err) {
			return ErrRecordsNotFound
		}
		return err
	}

	return nil
}

func (c *client) Select(ctx context.Context, dest interface{}, sql string, args ...interface{}) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	if err := c.selectInTx(ctxWithTimeout, dest, sql, args...); err != nil {
		if pgxscan.NotFound(err) {
			return ErrRecordsNotFound
		}
		return err
	}

	return nil
}

func (c *client) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	return c.execInTx(ctxWithTimeout, sql, args...)
}

func (c *client) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	return c.queryInTx(ctxWithTimeout, sql, args...)
}

func (c *client) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	return c.queryRowsInTx(ctxWithTimeout, sql, args...)
}

func (c *client) InTx(ctx context.Context, isoLevel pgx.TxIsoLevel, f func(ctx context.Context) error) error {
	conn, err := c.dbPool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: isoLevel})
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}

	// Устанавливаем транзакцию в контекст
	ctxWithTx := contextWithTx(ctx, tx)

	if err := f(ctxWithTx); err != nil {
		if err1 := tx.Rollback(ctxWithTx); err1 != nil {
			return fmt.Errorf("rolling back transaction: %v (original error: %w)", err1, err)
		}
		return err
	}

	if err := tx.Commit(ctxWithTx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

func (c *client) Ping(ctx context.Context) error {
	return c.dbPool.Ping(ctx)
}

func (c *client) Close() {
	c.dbPool.Close()
}

func (c *client) getInTx(ctx context.Context, dest interface{}, sql string, args ...interface{}) error {
	if tx := txFromContext(ctx); tx != nil {
		return c.scanApi.Get(ctx, tx, dest, sql, args...)
	}

	return c.scanApi.Get(ctx, c.dbPool, dest, sql, args...)
}

func (c *client) selectInTx(ctx context.Context, dest interface{}, sql string, args ...interface{}) error {
	if tx := txFromContext(ctx); tx != nil {
		return c.scanApi.Select(ctx, tx, dest, sql, args...)
	}

	return c.scanApi.Select(ctx, c.dbPool, dest, sql, args...)
}

func (c *client) execInTx(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if tx := txFromContext(ctx); tx != nil {
		return tx.Exec(ctx, sql, args...)
	}

	return c.dbPool.Exec(ctx, sql, args...)
}

func (c *client) queryInTx(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if tx := txFromContext(ctx); tx != nil {
		return tx.Query(ctx, sql, args...)
	}

	return c.dbPool.Query(ctx, sql, args...)
}

func (c *client) queryRowsInTx(ctx context.Context, sql string, args ...any) pgx.Row {
	if tx := txFromContext(ctx); tx != nil {
		return tx.QueryRow(ctx, sql, args...)
	}

	return c.dbPool.QueryRow(ctx, sql, args...)
}
