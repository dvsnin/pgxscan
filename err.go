package pgxscan

import "errors"

var (
	ErrRecordsNotFound = errors.New("no rows in result set")
)
