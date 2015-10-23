package loader

import (
	"github.com/jayjanssen/myq-tools/myqlib"
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const SPECIALENDSTRING string = "MYQTOOLSEND"

const (
	MYSQLCLI          string       = "mysql"

	// These next two must match
	END_STRING        string       = "MYQTOOLSEND"
	END_COMMAND        string       = "SELECT 'MYQTOOLSEND'"

	// The commands we send to the mysql cli
	STATUS_COMMAND    string = "SHOW GLOBAL STATUS"
	VARIABLES_COMMAND string = "SHOW GLOBAL VARIABLES"

	// prefix of SHOW VARIABLES keys, they are stored (if available) in the same map as the status variables
	VAR_PREFIX = "V_"

	LOADER_ERROR_KEY string = "LOADER_ERROR"
)

// Build the argument list
var MYSQLCLIARGS []string = []string{
	"-B", // Batch mode (tab-separated output)
	"-n", // Unbuffered
	"-N", // Skip column names
}

type Loader interface {
	getStatus() (chan myqlib.MyqSample, error)
	getVars() (chan myqlib.MyqSample, error)
	getInterval() time.Duration
}

// Given a loader, get a channel of myqstates being returned
func GetState(l Loader) (chan *myqlib.MyqState, error) {
	// First getVars, if possible
	var latestvars myqlib.MyqSample // whatever the last vars sample is will be here (may be empty)
	varsch, varserr := l.getVars()
	// return the error if getVars fails, but not if it's just due to a missing file
	if varserr != nil && varserr.Error() != "No file given" {
		// Serious error
		return nil, varserr
	}

	// Now getStatus
	var ch = make(chan *myqlib.MyqState)
	statusch, statuserr := l.getStatus()
	if statuserr != nil {
		return nil, statuserr
	}

	// Main status loop
	go func() {
		defer close(ch)

		prev := myqlib.NewMyqSample()
		var firstUptime int64
		for status := range statusch {
			// Init new state
			state := myqlib.NewMyqState()
			state.Cur = status

			// Only needed for File loaders really
			if firstUptime == 0 {
				firstUptime, _ = status.GetInt(`uptime`)
			}
			state.FirstUptime = firstUptime

			// Assign the prev
			if prev.Has(`uptime`) {
				state.Prev = prev

				// Calcuate timediff if there is a prev.  Only file loader?
				curup, _ := status.GetFloat(`uptime`)
				preup, _ := prev.GetFloat(`uptime`)
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
			latestvars.ForEach( func(k, v string) {
				newkey := fmt.Sprint(VAR_PREFIX, k)
				state.Cur.Set(newkey, v)
			})

			// Send the state
			ch <- &state

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
func (l FileLoader) harvestFile(filename string) (chan myqlib.MyqSample, error) {

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	var ch = make(chan myqlib.MyqSample)

	// The file scanning goes into the background
	go func() {
		defer file.Close()
		defer close(ch)
		parseSamples(file, ch, l.loaderInterval.getInterval())
	}()

	return ch, nil
}

func (l FileLoader) getStatus() (chan myqlib.MyqSample, error) {
	return l.harvestFile(l.statusFile)
}

func (l FileLoader) getVars() (chan myqlib.MyqSample, error) {
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

// Collect output from MYSQLCLI and send it back in a sample
func (l LiveLoader) harvestMySQL(command string) (chan myqlib.MyqSample, error) {
	// Make sure we have MYSQLCLI
	path, err := exec.LookPath(MYSQLCLI)
	if err != nil {
		return nil, err
	}

	var args = MYSQLCLIARGS
	if l.args != "" {
		args = append(args, strings.Split(l.args, ` `)...)
	}

	// Initialize the command
	cmd := exec.Command(path, args...)
	myqlib.CleanupSubcmd(cmd)

	// Collect Stderr in a buffer
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	// Create a pipe for Stdout
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	// Create a pipe for Stdin -- we input our command here every interval
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Handle if the subcommand exits
	go func() {
		err := cmd.Wait()
		if err != nil {
			os.Stderr.WriteString(stderr.String())
			os.Exit(1)
		}
	}()

	// feed the MYSQLCLI the given command to produce more output
	full_command := strings.Join( []string{command, END_COMMAND, "\n"}, "; " )
	send_command := func() {
		// We don't check if the write failed, it's assumed the cmd.Wait() above will catch the sub proc dying

		stdin.Write([]byte(full_command)) // command we're harvesting
	}
	// send the first command immediately
	send_command()

	// produce more output every interval
	ticker := time.NewTicker(l.getInterval())
	go func() {
		defer stdin.Close()
		for range ticker.C {
			send_command()
		}
	}()

	// parse samples in the background
	var ch = make(chan myqlib.MyqSample)
	go func() {
		defer close(ch)
		parseSamples(stdout, ch, l.loaderInterval.getInterval())
	}()

	// Got this far, the channel should start getting samples
	return ch, nil
}

func (l LiveLoader) getStatus() (chan myqlib.MyqSample, error) { return l.harvestMySQL(STATUS_COMMAND) }

func (l LiveLoader) getVars() (chan myqlib.MyqSample, error) { return l.harvestMySQL(VARIABLES_COMMAND) }
