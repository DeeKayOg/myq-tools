package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/jayjanssen/myq-tools/myqlib"
	"github.com/jayjanssen/myq-tools/myqlib/loader"
	"github.com/howeyc/gopass"
	"math"
	"os"
	"os/signal"
	"runtime/pprof"
	"sort"
	"syscall"
	"time"
)

// Exit codes
const (
	OK int = iota
	BAD_ARGS
	LOADER_ERROR
)

// Current Version (passed in on build)
var build_version string
var build_timestamp string

func main() {
	// Parse arguments
	help := flag.Bool("help", false, "this help text")
	version := flag.Bool("version", false, "print the version")
	debug := flag.Bool("debug", false, "debug output")

	// program utils
	profile := flag.String("profile", "", "enable profiling and store the result in this file")

	// output controls
	header := flag.Int64("header", 0, "repeat the header after this many data points (default: 0, autocalculates)")
	width := flag.Bool("width", false, "Truncate the output based on the width of the terminal (default: output lines wrap")


	interval := flag.Duration("interval", time.Second, "Time between samples (example: 1s or 1h30m)")
	flag.DurationVar(interval, "i", time.Second, "short for -interval")

	// Use input files instead of connecting to mysql
	statusfile := flag.String("file", "", "parse mysqladmin ext output file instead of connecting to mysql")
	flag.StringVar(statusfile, "f", "", "short for -file")
	varfile := flag.String("varfile", "", "parse mysqladmin variables file instead of connecting to mysql, for optional use with -file")
	flag.StringVar(varfile, "vf", "", "short for -varfile")

	// Connection info for live sampling of mysql
	user := flag.String("user", "root", "mysql user (default: root)")
	flag.StringVar(user, "u", "root", "short for -user")

	password := flag.String("password", "", "mysql password, leave blank to prompt")
	flag.StringVar(password, "p", "", "short for -password")

	ask_pass := flag.Bool("ask", false, "prompt for the mysql password")

	host := flag.String("host", "", "mysql hostname, default: localhost")
	flag.StringVar(host, "h", "", "short for -host")

	port := flag.Int64("port", 3306, "mysql port, default: 3306")
	flag.Int64Var(port, "P", 3306, "short for -port")

	socket := flag.String("socket", "", "mysqld unix socket file")
	flag.StringVar(socket, "S", "", "short for -socket")


	flag.Parse()

	// Enable profiling if set
	if *profile != "" {
		fmt.Println("Starting profiling to:", *profile)
		f, _ := os.Create(*profile)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()

		// Need to trap interrupts in order for the profile to flush
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigs
			pprof.StopCPUProfile()
			os.Exit(OK)
		}()

	}

	if *version {
		fmt.Printf("myq-tools %s (%s)\n", build_version, build_timestamp)
		os.Exit(OK)
	}

	if *debug {
		// Output the options
		fmt.Println("interval:", interval.String())

		fmt.Println( "statusfile:", *statusfile)
		fmt.Println( "varfile:", *varfile)

		fmt.Println( "user:", *user)
		fmt.Println( "password:", *password)
		fmt.Println( "host:", *host)
		fmt.Println( "port:", *port)

		fmt.Println( "socket:", *socket)

	}

	// Load default Views
	views := myqlib.DefaultViews()

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "myq-tools %s (%s)\n\n", build_version, build_timestamp)

		fmt.Fprintln(os.Stderr, "Usage:\n  myq_status [flags] <view>\n")
		fmt.Fprintln(os.Stderr, "Description:\n  iostat-like views for MySQL servers\n")

		fmt.Fprintln(os.Stderr, "Options:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nViews:")

		var view_usage bytes.Buffer

		var sorted_views []string
		for name, _ := range views {
			sorted_views = append(sorted_views, name)
		}
		sort.Strings(sorted_views)
		for _, name := range sorted_views {
			view := views[name]
			view_usage.WriteString(fmt.Sprint("  ", name, ": "))
			for shortst := range view.ShortHelp() {
				view_usage.WriteString(fmt.Sprint(shortst, "\n"))
			}
		}
		view_usage.WriteTo(os.Stderr)
		os.Exit(BAD_ARGS)
	}

	if flag.NArg() != 1 {
		flag.Usage()
	}

	if interval.Seconds() < 1 {
		fmt.Fprintln(os.Stderr, "Error: interval must be >= 1s")
		flag.Usage()
	} else if math.Mod(float64(interval.Nanoseconds()), 1000000000) != 0.0 {
		fmt.Fprintln(os.Stderr, "Warning: interval will be rounded to",
			fmt.Sprintf("%.0f", interval.Seconds()), "seconds")
	}

	view := flag.Arg(0)
	v, ok := views[view]
	if !ok {
		fmt.Fprintln(os.Stderr, "Error: view", view, "not found")
		flag.Usage()
	}

	if *help {
		var view_usage bytes.Buffer
		view_usage.WriteString(fmt.Sprint(`'`, view, `': `))
		for helpst := range v.Help() {
			view_usage.WriteString(fmt.Sprint(helpst, "\n"))
		}
		view_usage.WriteTo(os.Stderr)
		os.Exit(OK)
	}

	termheight, termwidth := myqlib.GetTermSize()

	// How many lines before printing a new header
	var headernum int64
	if *header != 0 {
		headernum = *header // Use the specified header count
	} else {
		headernum = termheight
	}

	// The Loader and Timecol we will use
	var l loader.Loader
	var err error

	if *statusfile != "" {
		// File given, load it (and the optional varfile)
		l = loader.NewFileLoader(*interval, *statusfile, *varfile)
		v.SetTimeCol(&myqlib.Runtime_col)
	} else {
		// No file given, this is a live collection and we use timestamps

		if *ask_pass {
		    fmt.Printf("Password: ")
		    *password = string(gopass.GetPasswd())
		}

		l, err = loader.NewSqlLoader(*interval, *user, *password, *host, *port)

		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(LOADER_ERROR)
		}

		v.SetTimeCol(&myqlib.Timestamp_col)
	}

	// Get channel that will feed us states from the loader
	states, err := loader.GetState(l)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(LOADER_ERROR)
	}

	// Apply selected view to output each sample
	lines := int64(0)
	var buf myqlib.FixedWidthBuffer
	if *width == true {
		buf.SetWidth(termwidth)
	}

	for state := range states {
		if state.HasError() {
			fmt.Fprintln(os.Stderr, state.GetError())
			continue
		}

		// Reprint a header whenever lines == 0
		if lines == 0 {
			headers := []string{}
			for headerln := range v.Header(state) {
				headers = append(headers, headerln)
			} // headers come out in reverse order
			for i := len(headers) - 1; i >= 0; i-- {
				buf.WriteString(fmt.Sprint(headers[i], "\n"))
				lines += 1
			}
		}

		// Output data
		for dataln := range v.Data(state) {
			buf.WriteString(fmt.Sprint(dataln, "\n"))
			lines += 1
		}
		buf.WriteTo(os.Stdout)
		buf.Reset()

		// Determine if we need to reset lines to 0 (and trigger a header)
		if lines/headernum >= 1 {
			lines = 0
			// Recalculate the size of the terminal now too
			termheight, termwidth = myqlib.GetTermSize()
			if *width == true {
				buf.SetWidth(termwidth)
			}
			if *header == 0 {
				headernum = termheight
			}
		}
	}

	os.Exit(OK)
}
