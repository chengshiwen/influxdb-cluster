package join

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/influxdata/influxdb/cmd/influxd-ctl/common"
	"github.com/influxdata/influxdb/services/meta"
)

// Command represents the program execution for "influxd-ctl join".
type Command struct {
	Stdout io.Writer
	Stderr io.Writer
	cOpts  *common.Options

	verbose bool
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
	if len(args) > 1 {
		return fmt.Errorf("unknown argument: %s", args[1])
	}
	addr := "localhost:8091"
	if len(args) == 1 {
		addr = args[0]
	}
	err = cmd.join(addr)
	return common.OperationExitedError(err)
}

// join to the cluster.
func (cmd *Command) join(addr string) error {
	fmt.Fprintf(cmd.Stdout, "Joining meta node at %s\n", addr)
	client := common.NewHTTPClient(cmd.cOpts)
	defer client.Close()

	ns := &meta.MetaNodeStatus{}
	if err := client.Status(addr, ns); err != nil {
		cmd.printUnableConnectError(addr, err)
		return nil
	}
	if ns.NodeType != meta.NodeTypeMeta {
		return fmt.Errorf("%s is not a meta node", addr)
	}

	hostname, _ := os.Hostname()
	metaAddr := fmt.Sprintf("%s:8091", hostname)
	dataAddr := fmt.Sprintf("%s:8088", hostname)
	fmt.Fprintf(cmd.Stdout, "Searching for meta node on %s...\n", metaAddr)
	fmt.Fprintf(cmd.Stdout, "Searching for data node on %s...\n", dataAddr)
	fmt.Fprintln(cmd.Stdout, "")

	mn := &meta.MetaNodeInfo{}
	if exists, _, _ := client.MetaNodeExists(metaAddr); exists {
		if err := client.JoinWithAddr(addr, metaAddr, mn); err != nil {
			cmd.printMetaNodeError(addr, metaAddr, err)
		}
	} else {
		cmd.printMetaNodeError(addr, metaAddr, nil)
	}

	dn := &meta.DataNodeInfo{}
	if exists, _, _ := client.DataNodeExists(dataAddr); exists {
		if err := client.AddDataWithAddr(addr, dataAddr, dn); err != nil {
			cmd.printDataNodeError(addr, dataAddr, err)
		}
	} else {
		cmd.printDataNodeError(addr, dataAddr, nil)
	}

	if mn.ID > 0 || dn.ID > 0 {
		if mn.ID == 1 {
			fmt.Fprintf(cmd.Stdout, "Successfully created cluster\n\n")
		} else {
			fmt.Fprintf(cmd.Stdout, "Successfully joined cluster\n\n")
		}
		if mn.ID > 0 {
			fmt.Fprintf(cmd.Stdout, "  * Added meta node %d at %s\n", mn.ID, mn.Addr)
		} else {
			fmt.Fprintf(cmd.Stdout, "  * No meta node added.  Run with -v to see more information\n")
		}
		if dn.ID > 0 {
			fmt.Fprintf(cmd.Stdout, "  * Added data node %d at %s\n", dn.ID, dn.TCPAddr)
		} else {
			fmt.Fprintf(cmd.Stdout, "  * No data node added.  Run with -v to see more information\n")
		}
		if mn.ID == 1 {
			fmt.Fprintln(cmd.Stdout, "")
			fmt.Fprintf(cmd.Stdout, "  To join additional nodes to this cluster, run the following command:\n\n")
			fmt.Fprintf(cmd.Stdout, "  influxd-ctl join %s\n", addr)
		}
	} else {
		fmt.Fprintln(cmd.Stdout, "Failed to join cluster.  Run with -v to see more information")
	}

	return nil
}

func (cmd *Command) printUnableConnectError(addr string, err error) {
	fmt.Fprintln(cmd.Stderr, "")
	fmt.Fprintf(cmd.Stderr, "Unable to connect to %s\n\n", addr)
	fmt.Fprintf(cmd.Stderr, "  operation timed out with error: %s\n\n", err)
	fmt.Fprintf(cmd.Stderr, "Common problems:\n\n")
	fmt.Fprintf(cmd.Stderr, "  * Make sure the address (host and port) are for a meta node\n")
	fmt.Fprintf(cmd.Stderr, "  * Verify a meta node is listening on %s\n", addr)
	fmt.Fprintf(cmd.Stderr, "  * Verify this host can connect to %s using TCP\n", addr)
}

func (cmd *Command) printMetaNodeError(addr, node string, err error) {
	if !cmd.verbose {
		return
	}
	if err != nil {
		fmt.Fprintf(cmd.Stderr, "Failed to add meta node %s!\n\n", node)
		fmt.Fprintf(cmd.Stderr, "  Reason:\n\n")
		fmt.Fprintf(cmd.Stderr, "    operation exited with error: %s\n\n", err)
	} else {
		fmt.Fprintf(cmd.Stdout, "No meta node found on %s!\n\n", node)
	}
	fmt.Fprintf(cmd.Stdout, "  If a meta node is running on this host,\n")
	fmt.Fprintf(cmd.Stdout, "  you may need to add it manually using the following command:\n\n")
	fmt.Fprintf(cmd.Stdout, "  influxd-ctl -bind %s add-meta <metaAddr:port>\n\n", addr)
	fmt.Fprintf(cmd.Stderr, "  Common problems:\n\n")
	fmt.Fprintf(cmd.Stderr, "    * The influxd-meta process is using a non-standard port (default 8091)\n")
	fmt.Fprintf(cmd.Stderr, "    * The influxd-meta process it not running.  Check the logs for startup errors\n\n")
}

func (cmd *Command) printDataNodeError(addr, node string, err error) {
	if !cmd.verbose {
		return
	}
	if err != nil {
		fmt.Fprintf(cmd.Stderr, "Failed to add data node %s!\n\n", node)
		fmt.Fprintf(cmd.Stderr, "  Reason:\n\n")
		fmt.Fprintf(cmd.Stderr, "    operation exited with error: %s\n\n", err)
	} else {
		fmt.Fprintf(cmd.Stdout, "No data node found on %s!\n\n", node)
	}
	fmt.Fprintf(cmd.Stdout, "  If a data node is running on this host,\n")
	fmt.Fprintf(cmd.Stdout, "  you may need to add it manually using the following command:\n\n")
	fmt.Fprintf(cmd.Stdout, "  influxd-ctl -bind %s add-data <dataAddr:port>\n\n", addr)
	fmt.Fprintf(cmd.Stderr, "  Common problems:\n\n")
	fmt.Fprintf(cmd.Stderr, "    * The influxd process is using a non-standard port (default 8088)\n")
	fmt.Fprintf(cmd.Stderr, "    * The influxd process it not running.  Check the logs for startup errors\n\n")
}

// parseFlags parses the command line flags.
func (cmd *Command) parseFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.BoolVar(&cmd.verbose, "v", false, "Print verbose information when joining")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, strings.TrimSpace(usage)) }
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

const usage = `
Usage: influxd-ctl [options] join [options] [addr]
    Adds a node to the cluster

Options:
  -v	Print verbose information when joining

Arguments:
    [addr] is the HTTP bind address of the meta node. (Optional: default localhost:8091)
`
