package add_meta

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/influxdata/influxdb/cmd/influxd-ctl/common"
	"github.com/influxdata/influxdb/services/meta"
)

// Command represents the program execution for "influxd-ctl add-meta".
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
	if len(args) == 0 {
		return errors.New("httpAddr value is empty")
	} else if len(args) > 1 {
		return fmt.Errorf("unknown argument: %s", args[1])
	}
	err = cmd.addMeta(args[0])
	return common.OperationExitedError(err)
}

// add meta addr.
func (cmd *Command) addMeta(addr string) error {
	client := common.NewHTTPClient(cmd.cOpts)
	data := url.Values{"addr": {addr}}
	resp, err := client.PostForm("/join", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return meta.DecodeErrorResponse(resp.Body)
	}

	mn := &meta.MetaNodeInfo{}
	if err = json.NewDecoder(resp.Body).Decode(mn); err != nil {
		return err
	}

	fmt.Fprintf(cmd.Stdout, "Added meta node %d at %s\n", mn.ID, mn.Addr)
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
Usage: influxd-ctl [options] add-meta <addr>
    Adds a meta node to the cluster

Arguments:
    <addr> is the HTTP bind address of the meta node.
`
