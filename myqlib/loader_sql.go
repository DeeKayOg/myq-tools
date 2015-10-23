package myqlib

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"time"
	"fmt"
	"log"
)


// type Loader interface {
// 	getStatus() (chan MyqSample, error)
// 	getVars() (chan MyqSample, error)
// 	getInterval() time.Duration
// }

// Load mysql status output from a mysqladmin output file
type SqlLoader struct {
	loaderInterval
	db *sql.DB
}

func NewSqlLoader(i time.Duration, user, pass, host string) *SqlLoader {
	db, err := sql.Open(`mysql`, fmt.Sprintf( "%s:%s@tcp(%s)/", user, pass, host ))
	if err != nil {
		log.Fatal( "Got ", err, "trying to connect to writer")
	}
	log.Println( "Connected!")
	// writer.SetMaxIdleConns(max_idle)
	// writer.SetMaxOpenConns(concurrency)

	return &SqlLoader{loaderInterval(i), db}
}

func (l SqlLoader) getSqlKeyValues(query string) (chan MyqSample, error) {
	var ch = make(chan MyqSample)

	// closure to query and get the KVs
	get_sample := func() {
		rows, err := l.db.Query( query )
		if err != nil {
			log.Print( "Query error: ", err )
		}
		defer rows.Close()

		timesample := make(MyqSample)

		for rows.Next() {
			var key, value string
			err = rows.Scan( &key, &value )
			if err != nil {
				log.Fatal( "Scan error: ", err )
			}
			// log.Println( key, " => ", value)
			timesample[key] = value
		}
		ch <- timesample
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

func (l SqlLoader) getStatus() (chan MyqSample, error) {
	return l.getSqlKeyValues( `select Variable_name, Variable_value from sys.metrics where Enabled='YES'` )
}

func (l SqlLoader) getVars() (chan MyqSample, error) {
	return l.getSqlKeyValues( `select lower(VARIABLE_NAME), VARIABLE_VALUE from information_schema.GLOBAL_VARIABLES` )
}
