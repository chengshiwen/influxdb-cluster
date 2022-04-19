package run

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/BurntSushi/toml"
)

// PrintConfigCommand represents the command executed by "influxd-meta config".
type PrintConfigCommand struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewPrintConfigCommand return a new instance of PrintConfigCommand.
func NewPrintConfigCommand() *PrintConfigCommand {
	return &PrintConfigCommand{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run parses and prints the current config loaded.
func (cmd *PrintConfigCommand) Run(args ...string) error {
	// Parse command flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	configPath := fs.String("config", "", "")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, printConfigUsage) }
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Parse config from path.
	opt := Options{ConfigPath: *configPath}
	config, err := cmd.parseConfig(opt.GetConfigPath())
	if err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	// Apply any environment variables on top of the parsed config
	if err := config.ApplyEnvOverrides(os.Getenv); err != nil {
		return fmt.Errorf("apply env config: %v", err)
	}

	// Validate the configuration.
	if err := config.Validate(); err != nil {
		return fmt.Errorf("%s. To generate a valid configuration file run `influxd-meta config > influxdb-meta.generated.conf`", err)
	}

	buf := new(bytes.Buffer)
	toml.NewEncoder(buf).Encode(config)
	output := buf.String()
	re := regexp.MustCompile(` *meta-tls-enabled[^\[\]]*?meta-internal-shared-secret.*?\n`)
	output = re.ReplaceAllString(output, "")
	fmt.Fprintln(cmd.Stdout, output)

	return nil
}

// ParseConfig parses the config at path.
// Returns a demo configuration if path is blank.
func (cmd *PrintConfigCommand) parseConfig(path string) (*Config, error) {
	config, err := NewDemoConfig()
	if err != nil {
		config = NewConfig()
	}

	if path == "" {
		return config, nil
	}

	fmt.Fprintf(os.Stderr, "Merging with configuration at: %s\n", path)

	if err := config.FromTomlFile(path); err != nil {
		return nil, err
	}
	return config, nil
}

var printConfigUsage = `Displays the default configuration.

Usage: influxd-meta config [flags]

    -config <path>
            Set the path to the initial configuration file.
            This defaults to the environment variable INFLUXDB_META_CONFIG_PATH,
            ~/.influxdb/influxdb-meta.conf, or /etc/influxdb/influxdb-meta.conf if a file
            is present at any of these locations.
            Disable the automatic loading of a configuration file using
            the null device (such as /dev/null).
`
