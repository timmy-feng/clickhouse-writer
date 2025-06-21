package chwriter

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func init() {
	caddy.RegisterModule(ClickHouseWriter{})
}

// ClickHouseWriter implements a log writer that performs buffered outputs to a ClickHouse database.
type ClickHouseWriter struct {
	DbName        string         `json:"db_name"`
	Table         string         `json:"table"`
	Host          string         `json:"host"`
	Username      string         `json:"username"`
	Password      string         `json:"password"`
	Port          string         `json:"port"`
	TLS           string         `json:"tls"`
	FlushInterval caddy.Duration `json:"flush_interval"`
}

// CaddyModule returns the Caddy module information.
func (ClickHouseWriter) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.logging.writers.clickhouse",
		New: func() caddy.Module { return new(ClickHouseWriter) },
	}
}

// Provision sets up the module.
func (writer *ClickHouseWriter) Provision(ctx caddy.Context) error {
	return nil
}

// WriterKey returns a unique key representing this nw.
func (writer *ClickHouseWriter) WriterKey() string {
	return fmt.Sprintf("%s:%s/%s.%s", writer.Host, writer.Port, writer.DbName, writer.Table)
}

func (writer *ClickHouseWriter) String() string {
	return writer.WriterKey()
}

// OpenWriter opens a new network connection.
func (writer *ClickHouseWriter) OpenWriter() (io.WriteCloser, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", writer.Host, writer.Port)},
		Auth: clickhouse.Auth{
			Database: writer.DbName,
			Username: writer.Username,
			Password: writer.Password,
		},
		TLS: &tls.Config{},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	clickhouseConn := clickhouseConn{
		Conn:          conn,
		table:         writer.Table,
		buffer:        []any{},
		bufferMu:      sync.Mutex{},
		flushInterval: time.Duration(writer.FlushInterval),
		done:          make(chan struct{}),
		wg:            sync.WaitGroup{},
	}
	go clickhouseConn.flushLoop()

	return &clickhouseConn, nil
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens. Syntax:
//
//	clickhouse {
//	    db_name <string>
//	    table <string>
//	    host <string>
//	    password <string>
//	    port <string>
//	    tls <string>
//	    flush_interval <duration>
//	}
func (nw *ClickHouseWriter) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for nesting := d.Nesting(); d.NextBlock(nesting); {
			switch d.Val() {
			case "db_name":
				if !d.Args(&nw.DbName) {
					return d.ArgErr()
				}

			case "table":
				if !d.Args(&nw.Table) {
					return d.ArgErr()
				}

			case "host":
				if !d.Args(&nw.Host) {
					return d.ArgErr()
				}

			case "port":
				if !d.Args(&nw.Port) {
					return d.ArgErr()
				}

			case "username":
				if !d.Args(&nw.Username) {
					return d.ArgErr()
				}

			case "password":
				if !d.Args(&nw.Password) {
					return d.ArgErr()
				}

			case "tls":
				if !d.Args(&nw.TLS) {
					return d.ArgErr()
				}

			case "flush_interval":
				if !d.NextArg() {
					return d.ArgErr()
				}
				flushInterval, err := caddy.ParseDuration(d.Val())
				if err != nil {
					return d.Errf("invalid duration: %s", d.Val())
				}
				if d.NextArg() {
					return d.ArgErr()
				}
				nw.FlushInterval = caddy.Duration(flushInterval)

			default:
				return d.Errf("unrecognized subdirective '%s'", d.Val())
			}
		}
	}
	return nil
}

// clickhouseConn wraps a ClickHouse connection and implements the io.WriteCloser interface.
type clickhouseConn struct {
	driver.Conn
	table         string
	buffer        []any
	bufferMu      sync.Mutex
	flushInterval time.Duration
	done          chan struct{}
	wg            sync.WaitGroup
}

func (conn *clickhouseConn) flush() error {
	conn.bufferMu.Lock()
	defer conn.bufferMu.Unlock()

	if len(conn.buffer) == 0 {
		return nil
	}

	ctx := context.Background()
	batch, err := conn.Conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", conn.table))
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	defer batch.Close()

	for _, data := range conn.buffer {
		if err := batch.AppendStruct(data); err != nil {
			return fmt.Errorf("failed to append struct: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	conn.buffer = []any{}
	return nil
}

func (conn *clickhouseConn) flushLoop() {
	conn.wg.Add(1)
	defer conn.wg.Done()

	for {
		select {
		case <-conn.done:
			return
		case <-time.After(conn.flushInterval):
			conn.flush()
		}
	}
}

func (conn *clickhouseConn) Write(b []byte) (n int, err error) {
	conn.bufferMu.Lock()
	defer conn.bufferMu.Unlock()

	var data any
	if err := json.Unmarshal(b, &data); err != nil {
		return 0, fmt.Errorf("failed to unmarshal data (clickhouse writer only accepts `format json`): %w", err)
	}
	conn.buffer = append(conn.buffer, data)

	return len(b), nil
}

func (conn *clickhouseConn) Close() error {
	close(conn.done)
	conn.wg.Wait()
	if err := conn.flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}
	return conn.Conn.Close()
}
