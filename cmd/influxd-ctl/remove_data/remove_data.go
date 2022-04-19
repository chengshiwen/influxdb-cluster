package remove_data

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/cmd/influxd-ctl/common"
	"github.com/influxdata/influxdb/services/meta"
)

// Command represents the program execution for "influxd-ctl remove-data".
type Command struct {
	Stdout io.Writer
	Stderr io.Writer
	cOpts  *common.Options

	force bool
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
	if err == flag.ErrHelp {
		return nil
	} else if err != nil {
		return err
	}
	if len(args) == 0 {
		return errors.New("addr value is empty")
	} else if len(args) > 1 {
		return fmt.Errorf("unknown argument: %s", args[1])
	}
	err = cmd.removeData(args[0])
	return common.OperationExitedError(err)
}

// remove data addr.
func (cmd *Command) removeData(addr string) error {
	client := common.NewHTTPClient(cmd.cOpts)
	data := url.Values{"addr": {addr}, "force": {strconv.FormatBool(cmd.force)}}
	resp, err := client.PostForm("/remove-data", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return meta.DecodeErrorResponse(resp.Body)
	}

	fmt.Fprintf(cmd.Stdout, "Removed data node at %s\n", addr)
	return nil
}

// parseFlags parses the command line flags.
func (cmd *Command) parseFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.BoolVar(&cmd.force, "force", false, "Force the removal of a data node.  Useful if the node is down.")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, strings.TrimSpace(usage)) }
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

const usage = `
Usage: influxd-ctl remove-data [options] <addr>
    Removes a data node from the cluster

Options:
  -force
    	Force the removal of a data node.  Useful if the node is down.

Arguments:
    <addr> is the TCP bind address of the data node.
`
