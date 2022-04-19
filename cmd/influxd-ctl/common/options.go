package common

// Options represents the command line options that can be parsed.
type Options struct {
	BindAddr   string
	BindTLS    bool
	SkipTLS    bool
	ConfigPath string
	AuthType   string
	Username   string
	Password   string
	Secret     string
}
