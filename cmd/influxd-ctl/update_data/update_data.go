package update_data

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/influxdata/influxdb/cmd/influxd-ctl/common"
	"github.com/influxdata/influxdb/services/meta"
)

// Command represents the program execution for "influxd-ctl update-data".
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
		return errors.New("old-addr value is empty")
	} else if len(args) == 1 {
		return errors.New("new-addr value is empty")
	} else if len(args) > 2 {
		return fmt.Errorf("unknown argument: %s", args[2])
	} else if args[1] == args[0] {
		return errors.New("new-addr and old-addr are the same")
	}
	err = cmd.updateData(args[0], args[1])
	return common.OperationExitedError(err)
}

// update data node.
func (cmd *Command) updateData(oldAddr, newAddr string) error {
	client := common.NewHTTPClient(cmd.cOpts)
	defer client.Close()
	dn := &meta.DataNodeInfo{}
	if err := client.UpdateData(oldAddr, newAddr, dn); err != nil {
		return err
	}
	fmt.Fprintf(cmd.Stdout, "Updated data node %d to %s\n", dn.ID, dn.TCPAddr)
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
Usage: influxd-ctl update-data <old-addr> <new-addr>
    Updates a data node address in the meta store

Arguments:
    <old-addr> is the old bind address of the data node.
    <new-addr> is the new bind address of the data node.
`
