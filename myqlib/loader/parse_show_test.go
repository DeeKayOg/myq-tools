package loader

import (
	"github.com/jayjanssen/myq-tools/myqlib"

	"fmt"
	"testing"
	"time"
)

func TestSingleSample(t *testing.T) {
	l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/mysqladmin.single", ""}
	samples, err := l.getStatus()
	if err != nil {
		t.Error(err)
	}

	// Check some types on some known metrics to verify autodetection
	sample := <-samples
	typeTests := map[string]string{
		"connections":                "int64",
		"compression":                "string",
		"wsrep_local_send_queue_avg": "float64",
		"binlog_snapshot_file":       "string",
	}

	for varname, expectedtype := range typeTests {
		i, ierr := sample.GetInt(varname)
		if ierr == nil {
			if expectedtype != "int64" {
				t.Fatal("Found integer, expected", expectedtype, "for", varname, "value: `", i, "`")
			} else {
				continue
			}
		}

		f, ferr := sample.GetFloat(varname)
		if ferr == nil {
			if expectedtype != "float64" {
				t.Fatal("Found float, expected", expectedtype, "for", varname, "value: `", f, "`")
			} else {
				continue
			}
		}

		s, serr := sample.GetString(varname)
		if serr == nil {
			if expectedtype != "string" {
				t.Fatal("Found string, expected", expectedtype, "for", varname, "value: `", s, "`")
			} else {
				continue
			}
		}
		fmt.Println(3)

	}

}

func TestTwoSamples(t *testing.T) {
	l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/mysqladmin.two", ""}
	samples, err := l.getStatus()

	if err != nil {
		t.Error(err)
	}

	checksamples(t, samples, 2)
}

func TestManySamples(t *testing.T) {
	if testing.Short() {
		return
	}

	l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/mysqladmin.lots", ""}
	samples, err := l.getStatus()

	if err != nil {
		t.Error(err)
	}

	checksamples(t, samples, 220)
}

func TestSingleBatchSample(t *testing.T) {
	l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/mysql.single", ""}
	samples, err := l.getStatus()
	if err != nil {
		t.Error(err)
	}

	// Check some types on some known metrics to verify autodetection
	sample := <-samples
	typeTests := map[string]string{
		"connections":                "int64",
		"compression":                "string",
		"wsrep_local_send_queue_avg": "float64",
		"binlog_snapshot_file":       "string",
	}

	for varname, expectedtype := range typeTests {
		i, ierr := sample.GetInt(varname)
		if ierr == nil {
			if expectedtype != "int64" {
				t.Fatal("Found integer, expected", expectedtype, "for", varname, "value: `", i, "`")
			} else {
				continue
			}
		}

		f, ferr := sample.GetFloat(varname)
		if ferr == nil {
			if expectedtype != "float64" {
				t.Fatal("Found float, expected", expectedtype, "for", varname, "value: `", f, "`")
			} else {
				continue
			}
		}

		s, serr := sample.GetString(varname)
		if serr == nil {
			if expectedtype != "string" {
				t.Fatal("Found string, expected", expectedtype, "for", varname, "value: `", s, "`")
			} else {
				continue
			}
		}
	}
}

func TestTwoBatchSamples(t *testing.T) {
	l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/mysql.two", ""}
	samples, err := l.getStatus()

	if err != nil {
		t.Error(err)
	}

	checksamples(t, samples, 2)
}

func TestManyBatchSamples(t *testing.T) {
	if testing.Short() {
		return
	}

	l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/mysql.lots", ""}
	samples, err := l.getStatus()

	if err != nil {
		t.Error(err)
	}

	checksamples(t, samples, 215)
}

func checksamples(t *testing.T, samples chan myqlib.MyqSample, expected int) {
	i := 0
	prev := myqlib.NewMyqSample()
	for sample := range samples {
		t.Log("New MyqSample", i, sample.Length(), sample.Get("uptime"))
		t.Log("\tPrev", i, prev.Length(), prev.Get("uptime"))

		if prev.Get("uptime") == sample.Get("uptime") {
			t.Fatal("previous has same uptime")
		}

		if prev.Length() > 0 && prev.Length() > sample.Length() {
			t.Log(prev.Get("uptime"), "(previous) had", prev.Length(), "keys.  Current current has", sample.Length())
			prev.ForEach(func(pkey, _ string) {
				if sample.Has(pkey) {
					t.Log("Missing", pkey, "from current sample")
				}
			})
			t.Fatal("")
		}
		prev = sample
		i++
	}

	if i != expected {
		t.Errorf("Got unexpected number of samples: %d", i)
	}
}

func TestTokuSample(t *testing.T) {
	l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/mysql.toku", ""}
	samples, err := l.getStatus()

	if err != nil {
		t.Error(err)
	}

	checksamples(t, samples, 2)
}

func BenchmarkParseStatus(b *testing.B) {
	for i := 0; i < b.N; i++ {
		l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/mysqladmin.single", ""}
		samples, err := l.getStatus()

		if err != nil {
			b.Error(err)
		}
		<-samples
	}
}

func BenchmarkParseStatusBatch(b *testing.B) {
	for i := 0; i < b.N; i++ {
		l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/mysql.single", ""}
		samples, err := l.getStatus()

		if err != nil {
			b.Error(err)
		}
		<-samples
	}
}

func BenchmarkParseVariablesBatch(b *testing.B) {
	for i := 0; i < b.N; i++ {
		l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/variables", ""}
		samples, err := l.getStatus()

		if err != nil {
			b.Error(err)
		}
		<-samples
	}
}

func BenchmarkParseVariablesTabular(b *testing.B) {
	for i := 0; i < b.N; i++ {
		l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/variables.tab", ""}
		samples, err := l.getStatus()

		if err != nil {
			b.Error(err)
		}
		<-samples
	}
}

func BenchmarkParseManyBatchSamples(b *testing.B) {
	for i := 0; i < b.N; i++ {
		l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/mysql.lots", ""}
		samples, err := l.getStatus()

		if err != nil {
			b.Error(err)
		}
		for j := 0; j <= 61; j++ {
			<-samples
		}
	}
}

func BenchmarkParseManySamples(b *testing.B) {
	for i := 0; i < b.N; i++ {
		l := FileLoader{loaderInterval(1 * time.Second), "../../testdata/mysqladmin.lots", ""}
		samples, err := l.getStatus()

		if err != nil {
			b.Error(err)
		}
		for j := 0; j <= 220; j++ {
			<-samples
		}
	}
}

func BenchmarkParseManySamplesLongInterval(b *testing.B) {
	for i := 0; i < b.N; i++ {
		l := FileLoader{loaderInterval(1 * time.Minute), "../../testdata/mysqladmin.lots", ""}
		samples, err := l.getStatus()

		if err != nil {
			b.Error(err)
		}

		j := 0
		for range samples {
			j++
		}
	}
}
