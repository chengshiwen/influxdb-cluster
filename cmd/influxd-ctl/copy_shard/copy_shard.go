package copy_shard

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

// Command represents the program execution for "influxd-ctl copy-shard".
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
		return errors.New("destAddr is required")
	} else if len(args) == 2 {
		return errors.New("shardID is required")
	} else if len(args) > 3 {
		return fmt.Errorf("unknown argument: %s", args[3])
	} else if args[1] == args[0] {
		return errors.New("destAddr and srcAddr are the same")
	}
	shardID, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil {
		return fmt.Errorf("error converting shardID to int: %s", args[2])
	}
	if shardID <= 0 {
		return errors.New("shardID must be greater than 0")
	}
	err = cmd.copyShard(args[0], args[1], shardID)
	return common.OperationExitedError(err)
}

// copy shard.
func (cmd *Command) copyShard(srcAddr, destAddr string, shardID uint64) error {
	client := common.NewHTTPClient(cmd.cOpts)
	defer client.Close()
	if err := client.CopyShard(srcAddr, destAddr, shardID); err != nil {
		return err
	}
	fmt.Fprintf(cmd.Stdout, "Copied shard %d from %s to %s\n", shardID, srcAddr, destAddr)
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
Usage: influxd-ctl copy-shard <srcAddr> <destAddr> <shardID>
    Copies a shard from src to dest

Arguments:
    <srcAddr> is the TCP bind address of the source data node.
    <destAddr> is the TCP bind address of the dest data node.
    <shardID> is the shard ID
`
