package truncate_shards

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/influxdata/influxdb/cmd/influxd-ctl/common"
)

// Command represents the program execution for "influxd-ctl truncate-shards".
type Command struct {
	Stdout io.Writer
	Stderr io.Writer
	cOpts  *common.Options

	delay time.Duration
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
	if len(args) > 0 {
		return fmt.Errorf("unexpected extra arguments: %v", args)
	}
	err = cmd.truncateShards()
	return common.OperationExitedError(err)
}

// truncates shards.
func (cmd *Command) truncateShards() error {
	client := common.NewHTTPClient(cmd.cOpts)
	defer client.Close()
	if err := client.TruncateShards(cmd.delay); err != nil {
		return err
	}
	fmt.Fprintln(cmd.Stdout, "Truncated shards.")
	return nil
}

// parseFlags parses the command line flags.
func (cmd *Command) parseFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.DurationVar(&cmd.delay, "delay", time.Minute, "duration from now to truncate shards")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, strings.TrimSpace(usage)) }
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

const usage = `
Usage: influxd-ctl [options] truncate-shards [-delay DELAY]
    Truncates current shards so that writes will include new nodes

Options:
  -delay duration
    	duration from now to truncate shards (default 1m0s)
`
