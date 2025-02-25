package record

import (
	"context"
	"encoding/json"
	"github.com/caryatid/cueball"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TODO used prepared statements
var getfmt = `SELECT worker, data FROM execution_state
WHERE id = $1
`

var persistfmt = `
INSERT INTO execution_log (id, done, inflight, version, worker, defer, data)
VALUES($1, $2, $3, $4, $5, $6, $7);
`

var loadworkfmt = `
SELECT worker, data
FROM execution_state WHERE NOT inflight AND NOT done
AND (defer IS NULL OR NOW() >= defer)
`

type pg struct {
	DB *pgxpool.Pool
}

func NewPG(ctx context.Context, dburl string) (cueball.Record, error) {
	var err error
	l := new(pg)
	config, err := pgxpool.ParseConfig(dburl)
	if err != nil {
		return nil, err
	}
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		dt, err := conn.LoadType(ctx, "stage")
		if err != nil {
			return err
		}
		conn.TypeMap().RegisterType(dt)
		dta, err := conn.LoadType(ctx, "_stage")
		if err != nil {
			return err
		}
		conn.TypeMap().RegisterType(dta)
		return nil
	}
	l.DB, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	return l, err
}

func (l *pg) Store(ctx context.Context, w mog.Operation) error {
	b, err := json.Marshal(w)
	if err != nil {
		return err
	}
	_, err = l.DB.Exec(ctx, persistfmt, w.ID().String(),
		w.Done(), w.Inflight(), w.Version(), W.Name(), w.Defer(), b)
	return err
}

func (l *pg) Scan(ctx context.Context, ch chan<- mog.Operation) error {
	var name string
	var data []byte
	rows, err := l.DB.Query(ctx, loadworkfmt)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(&name, &data); err != nil {
			return err
		}
		o, err := l.dum(name, data)
		if err != nil {
			return err
		}
		ch <- o
	}
	return nil
}

func (l *pg) Get(ctx context.Context, uuid uuid.UUID) (cueball.Worker, error) {
	var name string
	var data []byte
	if err := l.DB.QueryRow(ctx, getfmt, uuid).Scan(&name, &data); err != nil {
		return nil, err
	}
	return l.dum(name, data)
}

func (l *pg) Close() error {
	l.DB.Close()
	return nil
}

func (l *pg) dum(name string, data []byte) (Operation, error) { // data unmarshal
	o := gens(name)()
	if err := json.Unmarshal([]byte(data), o); err != nil {
		return nil, err
	}
	return o, nil
}
