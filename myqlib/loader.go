package myqlib

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"database/sql"
    _ "github.com/go-sql-driver/mysql"
)

type MySQLAdminCommand string

const (
	STATUS_COMMAND    MySQLAdminCommand = "SHOW GLOBAL STATUS"
	VARIABLES_COMMAND MySQLAdminCommand = "SHOW GLOBAL VARIABLES"
	// prefix of SHOW VARIABLES keys, they are stored (if available) in the same map as the status variables
	VAR_PREFIX = "V_"
)

type Loader interface {
	getStatus() (chan MyqSample, error)
	getVars() (chan MyqSample, error)
	getInterval() time.Duration
}

// MyqSamples are K->V maps
type MyqSample map[string]string

// Number of keys in the sample
func (s MyqSample) Length() int {
	return len(s)
}

// Get methods for the given key. Returns a value of the appropriate type (error is nil) or default value and an error if it can't parse
func (s MyqSample) getInt(key string) (int64, error) {
	val, ok := s[key]
	if !ok {
		return 0, errors.New("Key not found")
	}

	conv, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	} else {
		return conv, nil
	}
}
func (s MyqSample) getFloat(key string) (float64, error) {
	val, ok := s[key]
	if !ok {
		return 0.0, errors.New("Key not found")
	}

	conv, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0.0, err
	} else {
		return conv, nil
	}
}
func (s MyqSample) getString(key string) (string, error) {
	val, ok := s[key]
	if !ok {
		return "", errors.New("Key not found")
	}
	return val, nil // no errors possible here
}

// Same as above, just ignore the error
func (s MyqSample) getI(key string) int64 {
	i, _ := s.getInt(key)
	return i
}
func (s MyqSample) getF(key string) float64 {
	f, _ := s.getFloat(key)
	return f
}
func (s MyqSample) getStr(key string) string {
	str, _ := s.getString(key)
	return str
}

// Gets either a float or an int (check type of result), or an error
func (s MyqSample) getNumeric(key string) (interface{}, error) {
	if val, err := s.getInt(key); err != nil {
		return val, nil
	} else if val, err := s.getFloat(key); err != nil {
		return val, nil
	} else {
		return nil, errors.New("Value is not numeric")
	}
}

// MyqState contains the current and previous SHOW STATUS outputs.  Also SHOW VARIABLES.
// Prev might be nil
type MyqState struct {
	Cur, Prev   MyqSample
	SecondsDiff float64 // Difference between Cur and Prev
	FirstUptime int64   // Uptime of our first sample this run
}

// Given a loader, get a channel of myqstates being returned
func GetState(l Loader) (chan *MyqState, error) {
	// First getVars, if possible
	var latestvars MyqSample // whatever the last vars sample is will be here (may be empty)
	varsch, varserr := l.getVars()
	// return the error if getVars fails, but not if it's just due to a missing file
	if varserr != nil && varserr.Error() != "No file given" {
		// Serious error
		return nil, varserr
	}

	// Now getStatus
	var ch = make(chan *MyqState)
	statusch, statuserr := l.getStatus()
	if statuserr != nil {
		return nil, statuserr
	}

	// Main status loop
	go func() {
		defer close(ch)

		var prev MyqSample
		var firstUptime int64
		for status := range statusch {
			// Init new state
			state := new(MyqState)
			state.Cur = status

			// Only needed for File loaders really
			if firstUptime == 0 {
				firstUptime, _ = status.getInt(`uptime`)
			}
			state.FirstUptime = firstUptime

			// Assign the prev
			if prev != nil {
				state.Prev = prev

				// Calcuate timediff if there is a prev.  Only file loader?
				curup, _ := status.getFloat(`uptime`)
				preup, _ := prev.getFloat(`uptime`)
				state.SecondsDiff = curup - preup

				// Skip to the next sample if SecondsDiff is < the interval
				if state.SecondsDiff < l.getInterval().Seconds() {
					continue
				}
			}

			// If varserr is clear at this point, we're expecting some vars
			if varserr == nil {
				// get some new vars, or skip if the varsch is closed
				newvars, ok := <-varsch
				if ok {
					latestvars = newvars
				}
			}

			// Add latest vars to status with prefix
			for k, v := range latestvars {
				newkey := fmt.Sprint(VAR_PREFIX, k)
				state.Cur[newkey] = v
			}

			// Send the state
			ch <- state

			// Set the state for the next round
			prev = status
		}
	}()

	return ch, nil
}

type loaderInterval time.Duration

func (l loaderInterval) getInterval() time.Duration {
	return time.Duration(l)
}

// Load mysql status output from a mysqladmin output file
type FileLoader struct {
	loaderInterval
	statusFile    string
	variablesFile string
}

func NewFileLoader(i time.Duration, statusFile, varFile string) *FileLoader {
	return &FileLoader{loaderInterval(i), statusFile, varFile}
}
func (l FileLoader) harvestFile(filename string) (chan MyqSample, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	var ch = make(chan MyqSample)

	// The file scanning goes into the background
	go func() {
		defer file.Close()
		defer close(ch)
		parseSamples(file, ch, l.loaderInterval.getInterval())
	}()

	return ch, nil
}

func (l FileLoader) getStatus() (chan MyqSample, error) {
	return l.harvestFile(l.statusFile)
}

func (l FileLoader) getVars() (chan MyqSample, error) {
	if l.variablesFile != "" {
		return l.harvestFile(l.variablesFile)
	} else {
		return nil, errors.New("No file given")
	}
}

// SHOW output via mysqladmin on a live server
type LiveLoader struct {
	loaderInterval
	args string // other args for mysqladmin (like -u, -p, -h, etc.)
}

func NewLiveLoader(i time.Duration, args string) *LiveLoader {
	return &LiveLoader{loaderInterval(i), args}
}

// Collect output from mysqladmin and send it back in a sample
func (l LiveLoader) harvestMySQLAdmin(command MySQLAdminCommand) (chan MyqSample, error) {
	db, err := sql.Open("mysql", "")

	if err != nil {
		return nil, err
	}


	// fmt.Sprintf("%.0f", l.getInterval().Seconds()),
	var ch = make(chan MyqSample)
	ticker := time.NewTicker( l.getInterval() )

	// The file scanning goes into the background
	go func() {
		defer db.Close()
		defer close(ch)

		// Every time the ticker fires...
		for range ticker.C {
			// Run our query 
			rows, err := db.Query(string(command))
			if err != nil {
				fmt.Println(err.Error())
				continue
			}

			// get the results and put them into the sample
			timesample := make(MyqSample)
			for rows.Next() {
				var key, val string
				rows.Scan( &key, &val )

				// fmt.Println( key, "=>", val )
				timesample[strings.ToLower(key)] = val
			}
			ch <- timesample
		}
	}()

	return ch, nil
}

func (l LiveLoader) getStatus() (chan MyqSample, error) { return l.harvestMySQLAdmin(STATUS_COMMAND) }

func (l LiveLoader) getVars() (chan MyqSample, error) { return l.harvestMySQLAdmin(VARIABLES_COMMAND) }
