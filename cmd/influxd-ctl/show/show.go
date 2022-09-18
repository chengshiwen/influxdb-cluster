package show

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

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
	if err != nil {
		return nil
	}
	if len(args) > 0 {
		return fmt.Errorf("unexpected extra arguments: %v", args)
	}
	err = cmd.show()
	return common.OperationExitedError(err)
}

// show cluster nodes.
func (cmd *Command) show() error {
	client := common.NewHTTPClient(cmd.cOpts)
	defer client.Close()
	ci := &meta.ClusterInfo{}
	if err := client.ShowCluster(ci); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 8, 8, 2, '\t', 0)

	fmt.Fprintln(cmd.Stdout, "Data Nodes")
	fmt.Fprintln(cmd.Stdout, "==========")
	fmt.Fprintln(tw, strings.Join([]string{"ID", "TCP Address", "Version"}, "\t"))
	for _, n := range ci.Data {
		fmt.Fprintf(tw, "%d\t%s\t%s\n", n.ID, n.TCPAddr, n.Version)
	}
	tw.Flush()
	fmt.Fprintln(cmd.Stdout, "")

	fmt.Fprintln(cmd.Stdout, "Meta Nodes")
	fmt.Fprintln(cmd.Stdout, "==========")
	fmt.Fprintln(tw, strings.Join([]string{"ID", "TCP Address", "Version"}, "\t"))
	for _, n := range ci.Meta {
		fmt.Fprintf(tw, "%d\t%s\t%s\n", n.ID, n.Addr, n.Version)
	}
	tw.Flush()
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
