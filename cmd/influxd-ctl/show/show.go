package show

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/influxdata/influxdb/cmd/influxd-ctl/common"
	"github.com/influxdata/influxdb/services/meta"
)

// Command represents the program execution for "influxd-ctl show".
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
	if err == flag.ErrHelp {
		return nil
	} else if err != nil {
		return err
	}
	if len(args) > 0 {
		return fmt.Errorf("unexpected extra arguments: %v", args)
	}
	err = cmd.show()
	return common.OperationExitedError(err)
}

// show cluster info.
func (cmd *Command) show() error {
	client := common.NewHTTPClient(cmd.cOpts)
	resp, err := client.Get("/show-cluster")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return meta.DecodeErrorResponse(resp.Body)
	}

	ci := &meta.ClusterInfo{}
	if err = json.NewDecoder(resp.Body).Decode(ci); err != nil {
		return err
	}

	fmt.Fprintln(cmd.Stdout, "Data Nodes")
	fmt.Fprintln(cmd.Stdout, "==========")
	fmt.Fprintln(cmd.Stdout, "ID\tTCP Address")
	for _, n := range ci.Data {
		fmt.Fprintln(cmd.Stdout, n.ID, "\t", n.TCPAddr)
	}
	fmt.Fprintln(cmd.Stdout, "")

	fmt.Fprintln(cmd.Stdout, "Meta Nodes")
	fmt.Fprintln(cmd.Stdout, "==========")
	fmt.Fprintln(cmd.Stdout, "ID\tTCP Address")
	for _, n := range ci.Meta {
		fmt.Fprintln(cmd.Stdout, n.ID, "\t", n.Addr)
	}
	fmt.Fprintln(cmd.Stdout, "")

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
Usage: influxd-ctl [options] show
    Lists nodes with the cluster
`
