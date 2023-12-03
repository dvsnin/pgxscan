package pgxscan

import (
	"fmt"
	"strings"
	"time"
)

type Config struct {
	Host     string `json:"host" mapstructure:"host"`
	Port     string `json:"port" mapstructure:"port"`
	Name     string `json:"name" mapstructure:"name"`
	User     string `json:"user" mapstructure:"user"`
	Password string `json:"password" mapstructure:"password"`
	SSLMode  string `json:"sslMode" mapstructure:"sslMode"`

	// Query timeout, default is 5s
	QueryTimeout time.Duration `json:"queryTimeout" mapstructure:"queryTimeout"`

	// Minimum number of idle connections (inactive connections that remain open)
	PoolMinConnections string `json:"poolMinConnections" mapstructure:"poolMinConnections"`

	// Maximum number of connections
	PoolMaxConnections string `json:"poolMaxConnections" mapstructure:"poolMaxConnections"`

	// The duration for which a connection will live before being closed
	PoolMaxConnLife time.Duration `json:"poolMaxConnLife" mapstructure:"poolMaxConnLife"`

	// Time after which an idle connection will be closed
	PoolMaxConnIdle time.Duration `json:"poolMaxConnIdle" mapstructure:"poolMaxConnIdle"`

	// Checks each connection taken from the pool to ensure it's alive
	// Otherwise, pgx will drop it and provide a new one
	// Disabling this check speeds up queries,
	// but if a bad connection is encountered, the query will fail with an error
	EnableBeforeAcquirePing bool `json:"enableBeforeAcquirePing" mapstructure:"enableBeforeAcquirePing"`

	// Allows the scanner to ignore database columns that do not exist in the destination
	AllowUnknownColumns bool `json:"allowUnknownColumns" mapstructure:"allowUnknownColumns"`
}

func (c *Config) Valid() error {
	if c.Host == "" {
		return fmt.Errorf("'host' was not set")
	}
	if c.Port == "" {
		return fmt.Errorf("'port' was not set")
	}
	if c.Name == "" {
		return fmt.Errorf("'name' was not set")
	}
	if c.User == "" {
		return fmt.Errorf("'user' was not set")
	}
	if c.Password == "" {
		return fmt.Errorf("'password' was not set")
	}
	return nil
}

func dbDSN(cfg Config) string {
	vals := dbValues(cfg)
	p := make([]string, 0, len(vals))
	for k, v := range vals {
		p = append(p, fmt.Sprintf("%s=%s", k, v))
	}
	return strings.Join(p, " ")
}

func setIfNotEmpty(m map[string]string, key, val string) {
	if val != "" {
		m[key] = val
	}
}

func setIfPositiveDuration(m map[string]string, key string, d time.Duration) {
	if d > 0 {
		m[key] = d.String()
	}
}

func dbValues(cfg Config) map[string]string {
	p := map[string]string{}
	setIfNotEmpty(p, "dbname", cfg.Name)
	setIfNotEmpty(p, "user", cfg.User)
	setIfNotEmpty(p, "host", cfg.Host)
	setIfNotEmpty(p, "port", cfg.Port)
	setIfNotEmpty(p, "sslmode", cfg.SSLMode)
	setIfNotEmpty(p, "password", cfg.Password)
	setIfNotEmpty(p, "pool_min_conns", cfg.PoolMinConnections)
	setIfNotEmpty(p, "pool_max_conns", cfg.PoolMaxConnections)
	setIfPositiveDuration(p, "pool_max_conn_lifetime", cfg.PoolMaxConnLife)
	setIfPositiveDuration(p, "pool_max_conn_idle_time", cfg.PoolMaxConnIdle)
	return p
}
