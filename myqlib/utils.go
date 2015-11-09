package myqlib

import (
	"bytes"
	"os/exec"
	"reflect"
	"syscall"
)

// Set OS-specific SysProcAttrs if they exist
func CleanupSubcmd(c *exec.Cmd) {
	// Send the subprocess a SIGTERM when we exit
	attr := new(syscall.SysProcAttr)

	r := reflect.ValueOf(attr)
	f := reflect.Indirect(r).FieldByName(`Pdeathsig`)

	if f.IsValid() {
		f.Set(reflect.ValueOf(syscall.SIGTERM))
		c.SysProcAttr = attr
	}
}

//
type FixedWidthBuffer struct {
	bytes.Buffer
	maxwidth int
}

func (b *FixedWidthBuffer) SetWidth(w int) (changed bool) {
	if w != b.maxwidth {
		b.maxwidth = w
		return true
	}
	return false
}
func (b *FixedWidthBuffer) WriteString(s string) (n int, err error) {
	runes := bytes.Runes([]byte(s))
	if b.maxwidth != 0 && len(runes) > int(b.maxwidth) {
		return b.Buffer.WriteString(string(runes[:b.maxwidth]))
	} else {
		return b.Buffer.WriteString(s)
	}
}
