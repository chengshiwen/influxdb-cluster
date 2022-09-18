package leave

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/influxdata/influxdb/cmd/influxd-ctl/common"
)

// Command represents the program execution for "influxd-ctl leave".
type Command struct {
	Stdout io.Writer
	Stderr io.Writer
	cOpts  *common.Options

	yes bool
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
	err = cmd.leave()
	return common.OperationExitedError(err)
}

// leave from the cluster.
func (cmd *Command) leave() error {
	client := common.NewHTTPClient(cmd.cOpts)
	defer client.Close()
	hostname, _ := os.Hostname()

	dataRemoved := false
	dataAddr := fmt.Sprintf("%s:8088", hostname)
	fmt.Fprintf(cmd.Stdout, "Searching for data node on %s...\n", dataAddr)
	if exists, joined, metaAddrs := client.DataNodeExists(dataAddr); exists && joined {
		err := cmd.prompt(fmt.Sprintf("Remove data node %s from the cluster", dataAddr))
		if err == nil {
			if err = client.RemoveDataWithAddr(metaAddrs[0], dataAddr, false); err != nil {
				fmt.Fprintf(cmd.Stdout, "  * Operation exited with error: %s\n", err)
			} else {
				dataRemoved = true
				fmt.Fprintf(cmd.Stdout, "Removed %s from the cluster\n", dataAddr)
			}
		} else if err != common.ErrPromptNotYes {
			fmt.Fprintf(cmd.Stdout, "  * Prompt error: %s\n", err)
		}
	} else if exists {
		fmt.Fprintf(cmd.Stdout, "  * Data node at %s is not part of a cluster\n", dataAddr)
	} else {
		fmt.Fprintf(cmd.Stdout, "  * No data node found.\n")
	}

	metaRemoved := false
	metaAddr := fmt.Sprintf("%s:8091", hostname)
	fmt.Fprintf(cmd.Stdout, "Searching for meta node on %s...\n", metaAddr)
	if exists, joined, _ := client.MetaNodeExists(metaAddr); exists && joined {
		err := cmd.prompt(fmt.Sprintf("Remove meta node %s from the cluster", metaAddr))
		if err == nil {
			if err = client.Leave(metaAddr); err != nil {
				fmt.Fprintf(cmd.Stdout, "  * Operation exited with error: %s\n", err)
			} else {
				metaRemoved = true
			}
		} else if err != common.ErrPromptNotYes {
			fmt.Fprintf(cmd.Stdout, "  * Prompt error: %s\n", err)
		}
	} else if exists {
		fmt.Fprintf(cmd.Stdout, "  * Meta node at %s is not part of a cluster\n", metaAddr)
	} else {
		fmt.Fprintf(cmd.Stdout, "  * No meta node found.\n")
	}

	if dataRemoved || metaRemoved {
		fmt.Fprintln(cmd.Stdout, "")
		fmt.Fprintf(cmd.Stdout, "Successfully left cluster\n\n")
		if dataRemoved {
			fmt.Fprintf(cmd.Stdout, "  * Removed data node %s from cluster\n", dataAddr)
		} else {
			fmt.Fprintf(cmd.Stdout, "  * No data node removed from cluster\n")
		}
		if metaRemoved {
			fmt.Fprintf(cmd.Stdout, "  * Removed meta node %s from cluster\n", metaAddr)
		} else {
			fmt.Fprintf(cmd.Stdout, "  * No meta node removed from cluster\n")
		}
	} else {
		fmt.Fprintln(cmd.Stdout, "No nodes removed from cluster.")
	}

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
	fs.BoolVar(&cmd.yes, "y", false, "Assume Yes to all prompts")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, strings.TrimSpace(usage)) }
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

const usage = `
Usage: influxd-ctl [options] leave [options]
    Removes a node from the cluster. 

Options:
  -y	Assume Yes to all prompts
`
