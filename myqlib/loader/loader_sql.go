package loader

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jayjanssen/myq-tools/myqlib"
	"time"
	"errors"
)

// Load mysql status output from a mysqladmin output file
type SqlLoader struct {
	loaderInterval
	db *sql.DB
	status_table_name string
}

func NewSqlLoader(i time.Duration, user, pass, host string, port int64) (*SqlLoader, error) {
	var cstr string
	cstr = user
	if pass != `` {
		cstr += `:` + pass
	}
	db, err := sql.Open(`mysql`, fmt.Sprintf("%s@tcp(%s:%d)/", cstr, host, port))
	if err != nil {
		return nil, err
	}
	// writer.SetMaxIdleConns(max_idle)
	// writer.SetMaxOpenConns(concurrency)

	// Test the connection to verify credentials now
	_, err = db.Query(`SELECT 1`)
	if err != nil {
		return nil, err
	}

	// Check which table is available, prefer the first
	tables := []string{`sys.metrics`,`information_schema.global_status`}
	var table *string
	for _, t := range tables {
		_, err = db.Query(`desc ` + t)
		if err == nil {
			table = &t
			break
		} else {
			fmt.Println( "Couldn't find ", t)
		}
	}
	if table == nil {
		return nil, errors.New("Couldn't find a status table to query, use a MySQL version with the INFORMATION_SCHEMA")
	}

	return &SqlLoader{loaderInterval(i), db, *table}, nil
}

func (l SqlLoader) getSqlKeyValues(query string) (chan myqlib.MyqSample, error) {
	var ch = make(chan myqlib.MyqSample)

	// closure to query and get the KVs
	get_sample := func() {
		timesample := myqlib.NewMyqSample()
		defer func() {
			ch <- timesample
		}()

		rows, err := l.db.Query(query)
		if err != nil {
			timesample.SetError(err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var key, value string
			err = rows.Scan(&key, &value)
			if err != nil {
				timesample.SetError(err)
				return
			}
			// log.Println( key, " => ", value)
			timesample.Set(key, value)
		}
	}

	// Run the first query
	go get_sample()

	// re-run the query every interval
	ticker := time.NewTicker(l.getInterval())
	go func() {
		for range ticker.C {
			get_sample()
		}
	}()

	return ch, nil
}

func (l SqlLoader) getStatus() (chan myqlib.MyqSample, error) {
	return l.getSqlKeyValues(`select lower(variable_name), variable_value from ` + l.status_table_name )
}

func (l SqlLoader) getVars() (chan myqlib.MyqSample, error) {
	return l.getSqlKeyValues(`select lower(variable_name), variable_name from information_schema.GLOBAL_VARIABLES`)
}
