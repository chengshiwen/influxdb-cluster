// Package help is the help subcommand of the influxd-ctl command.
package help

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// Command displays help for command-line sub-commands.
type Command struct {
	Stdout io.Writer
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stdout: os.Stdout,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fmt.Fprintln(cmd.Stdout, strings.TrimSpace(usage))
	return nil
}

const usage = `
Usage: influxd-ctl [options] <command> [options] [<args>]

Available commands are:
   add-data            Add a data node
   add-meta            Add a meta node
   copy-shard          Copy a shard between data nodes
   join                Join a meta or data node
   leave               Remove a meta or data node
   remove-data         Remove a data node
   remove-meta         Remove a meta node
   remove-shard        Remove a shard from a data node
   show                Show cluster members
   show-shards         Shows the shards in a cluster
   update-data         Update a data node
   token               Generates a signed JWT token
   truncate-shards     Truncate current shards

Options:

  -auth-type string
    	Type of authentication to use (none, basic, jwt) (default "none")
  -bind string
    	Bind HTTP address of a meta node (default "localhost:8091")
  -bind-tls
    	Use TLS
  -k	Skip certificate verification (ignored without -bind-tls)
  -pwd string
    	Password (ignored without -auth-type basic)
  -secret string
    	JWT shared secret (ignored without -auth-type jwt)
  -user string
    	User name (ignored without -auth-type basic | jwt)
`
