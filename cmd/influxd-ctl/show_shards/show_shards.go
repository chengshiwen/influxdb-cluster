package show_shards

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/cmd/influxd-ctl/common"
	"github.com/influxdata/influxdb/services/meta"
)

// Command represents the program execution for "influxd-ctl show-shards".
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
	if len(args) > 0 {
		return fmt.Errorf("unexpected extra arguments: %v", args)
	}
	err = cmd.showShards()
	return common.OperationExitedError(err)
}

// show shards.
func (cmd *Command) showShards() error {
	client := common.NewHTTPClient(cmd.cOpts)
	defer client.Close()
	var shardInfos []meta.ClusterShardInfo
	if err := client.ShowShards(cmd.verbose, &shardInfos); err != nil {
		return err
	}

	fmt.Fprintln(cmd.Stdout, "Shards")
	fmt.Fprintln(cmd.Stdout, "==========")
	tw := tabwriter.NewWriter(cmd.Stdout, 1, 1, 2, ' ', 0)
	fmt.Fprintln(tw, strings.Join([]string{"ID", "Database", "Retention Policy",
		"Desired Replicas", "Shard Group", "Start", "End", "Expires", "Owners"}, "\t"))
	for _, si := range shardInfos {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n", si.ID, si.Database, si.RetentionPolicy, si.ReplicaN,
			si.ShardGroupID, common.FormatRFC3339(si.StartTime), cmd.formatEndTime(si.EndTime, si.TruncatedAt),
			common.FormatRFC3339(si.ExpireTime), cmd.formatOwners(si.Owners))
	}
	tw.Flush()
	return nil
}

func (cmd *Command) formatEndTime(endTime, truncatedAt time.Time) string {
	if !truncatedAt.IsZero() {
		return fmt.Sprintf("%s*", common.FormatRFC3339Nano(truncatedAt))
	}
	return common.FormatRFC3339(endTime)
}

func (cmd *Command) formatOwners(owners []*meta.ShardOwnerInfo) string {
	var infos []string
	for _, oi := range owners {
		var fields []string
		fields = append(fields, fmt.Sprintf("ID:%d", oi.ID))
		fields = append(fields, fmt.Sprintf("TCPAddr:%s", oi.TCPAddr))
		if cmd.verbose {
			fields = append(fields, fmt.Sprintf("State:%s", oi.State))
			fields = append(fields, fmt.Sprintf("LastModified:%s", oi.LastModified.UTC().Format(time.RFC3339Nano)))
			fields = append(fields, fmt.Sprintf("Size:%d", oi.Size))
			if oi.Err != "" {
				fields = append(fields, fmt.Sprintf("Err:%s", oi.Err))
			} else {
				fields = append(fields, "Err:nil")
			}
		}
		info := fmt.Sprintf("{%s}", strings.Join(fields, " "))
		infos = append(infos, info)
	}
	return fmt.Sprintf("%v", infos)
}

// parseFlags parses the command line flags.
func (cmd *Command) parseFlags(args []string) ([]string, error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.BoolVar(&cmd.verbose, "v", false, "displays detailed shard info")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, strings.TrimSpace(usage)) }
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs.Args(), nil
}

const usage = `
Usage: influxd-ctl show-shards [options]
    Lists shards with the cluster

Options:
  -v	displays detailed shard info
`
