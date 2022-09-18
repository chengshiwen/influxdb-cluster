package remove_shard

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/cmd/influxd-ctl/common"
)

// Command represents the program execution for "influxd-ctl remove-shard".
type Command struct {
	Stdout io.Writer
	Stderr io.Writer
	cOpts  *common.Options
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
	if len(args) == 0 {
		return errors.New("srcAddr is required")
	} else if len(args) == 1 {
		return errors.New("shardID is required")
	} else if len(args) > 2 {
		return fmt.Errorf("unknown argument: %s", args[2])
	}
	shardID, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return fmt.Errorf("error converting shardID to int: %s", args[1])
	}
	if shardID <= 0 {
		return errors.New("shardID must be greater than 0")
	}
	err = cmd.removeShard(args[0], shardID)
	return common.OperationExitedError(err)
}

// remove shard.
func (cmd *Command) removeShard(srcAddr string, shardID uint64) error {
	client := common.NewHTTPClient(cmd.cOpts)
	defer client.Close()
	if err := client.RemoveShard(srcAddr, shardID); err != nil {
		return err
	}
	fmt.Fprintf(cmd.Stdout, "Removed shard %d from %s\n", shardID, srcAddr)
	return nil
}

// parseFlags parses the command line flags.
func (cmd *Command) parseFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, strings.TrimSpace(usage)) }
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

const usage = `
Usage: influxd-ctl remove-shard <srcAddr> <shardID>
    Removes a shard from a node

Arguments:
    <srcAddr> is the TCP bind address of the source data node.
    <shardID> is the shard ID
`
