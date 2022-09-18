package remove_meta

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/influxdata/influxdb/cmd/influxd-ctl/common"
	"github.com/influxdata/influxdb/services/meta"
)

// Command represents the program execution for "influxd-ctl remove-meta".
type Command struct {
	Stdout io.Writer
	Stderr io.Writer
	cOpts  *common.Options

	force   bool
	tcpAddr string
	yes     bool
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
		return errors.New("addr is required")
	} else if len(args) > 1 {
		return fmt.Errorf("unknown argument: %s", args[1])
	}
	if cmd.force && cmd.tcpAddr == "" {
		return errors.New("-tcpAddr is required with -force")
	}
	err = cmd.removeMeta(args[0])
	return common.OperationExitedError(err)
}

// remove meta node.
func (cmd *Command) removeMeta(addr string) error {
	hint := fmt.Sprintf("Remove %s from the cluster", addr)
	if cmd.force {
		hint = fmt.Sprintf("Force remove %s from the cluster", addr)
	}
	err := cmd.prompt(hint)
	if err == common.ErrPromptNotYes {
		return nil
	} else if err != nil {
		return err
	}

	client := common.NewHTTPClient(cmd.cOpts)
	defer client.Close()
	if cmd.force && cmd.tcpAddr != "" {
		if err = client.Remove(addr, cmd.force, cmd.tcpAddr); err != nil {
			return err
		}
	} else {
		ns := &meta.MetaNodeStatus{}
		if err = client.Status(addr, ns); err != nil {
			return err
		}
		if ns.NodeType != meta.NodeTypeMeta {
			return fmt.Errorf("%s is not a meta node", addr)
		}
		if ns.Leader == "" {
			return fmt.Errorf("%s is not part of a cluster", addr)
		}
		if err = client.Leave(addr); err != nil {
			return err
		}
	}
	fmt.Fprintf(cmd.Stdout, "Removed meta node at %s\n", addr)
	return nil
}

func (cmd *Command) prompt(hint string) error {
	if !cmd.yes {
		fmt.Fprintf(cmd.Stdout, "%s [y/N]: ", hint)
		scan := bufio.NewScanner(os.Stdin)
		scan.Scan()
		if scan.Err() != nil {
			return fmt.Errorf("error reading STDIN: %v", scan.Err())
		}
		if strings.ToLower(scan.Text()) != "y" {
			return common.ErrPromptNotYes
		}
	}
	return nil
}

// parseFlags parses the command line flags.
func (cmd *Command) parseFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.BoolVar(&cmd.force, "force", false, "Force the removal of a meta node.  Useful if the node is down.")
	fs.StringVar(&cmd.tcpAddr, "tcpAddr", "", "The TCP address of the node to remove.")
	fs.BoolVar(&cmd.yes, "y", false, "Assume Yes to all prompts")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, strings.TrimSpace(usage)) }
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

const usage = `
Usage: influxd-ctl [options] remove-meta [options] <addr>
    Removes a meta node from the cluster.  By default, the local meta node will be
    contacted to remove the specified node.  If -bind is specified, the bound address will be used.

    To force remove a meta node, bind to an existing meta node and specify
    the -tcpAddr and -force options along with the meta nodes HTTP Addr.  Force removing a meta
    node should only be used if the meta node is no longer reachable and is unrecoverable.
    If the node is restarted after being force removed, it may interfere with the cluster.

Options:
  -force
    	Force the removal of a meta node.  Useful if the node is down.
  -tcpAddr string
    	The TCP address of the node to remove.
  -y	Assume Yes to all prompts

Examples:
    # Remove the local meta node running on :8091
    $ influxd-ctl remove-meta localhost:8091

    # Remove the meta node running on meta2:8091
    $ influxd-ctl remove-meta meta2:8091

    # Force remove the meta node that existed at meta2:8091 using the local meta node
    $ influxd-ctl remove-meta -force -tcpAddr meta2:8089 meta2:8091

    # Force remove the meta node that existed at meta2:8091 using the meta1:8091
    $ influxd-ctl -bind meta1:8091 remove-meta -force -tcpAddr meta2:8089 meta2:8091
`
