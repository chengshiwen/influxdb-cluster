package token

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/influxdata/influxdb/cmd/influxd-ctl/common"
	"github.com/influxdata/influxdb/pkg/httputil"
	"github.com/influxdata/influxdb/pkg/jwtutil"
)

// Command represents the program execution for "influxd-ctl token".
type Command struct {
	Stdout io.Writer
	Stderr io.Writer
	cOpts  *common.Options

	exp time.Duration
}

// NewCommand return a new instance of Command.
func NewCommand(cOpts *common.Options) *Command {
	return &Command{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		cOpts:  cOpts,
	}
}

// Run executes the program.
func (cmd *Command) Run(args ...string) error {
	args, err := cmd.parseFlags(args)
	if err != nil {
		return nil
	}
	if cmd.cOpts.AuthType != httputil.AuthTypeJWT {
		return errors.New("tokens can only be created when using bearer authentication")
	}
	err = cmd.token()
	return common.OperationExitedError(err)
}

// generate signed JWT token.
func (cmd *Command) token() error {
	signed, err := jwtutil.SignedString(cmd.cOpts.Username, cmd.cOpts.Secret, cmd.exp)
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.Stdout, signed)
	return nil
}

// parseFlags parses the command line flags.
func (cmd *Command) parseFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.DurationVar(&cmd.exp, "exp", time.Minute, "token will expire after this duration")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, strings.TrimSpace(usage)) }
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

const usage = `
Usage: influxd-ctl -auth-type jwt -secret <shared secret> token [options]
    Generates a signed JWT token

Options:
  -exp duration
    	token will expire after this duration (default 1m0s)
`
