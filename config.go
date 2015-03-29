package mixpanels3

import "fmt"

type Config struct {
	From   string
	To     string
	Events []string
	Key    string
	Secret string
	Bucket string
	Prefix string
	Split  bool
}

func (c *Config) GetEffectiveS3Prefix() string {
	return fmt.Sprintf("%s%s_%s_%s/", c.Prefix, c.Key, c.From, c.To)
}

func (c *Config) GetTmpFilename() string {
	return fmt.Sprintf("mixpanel_%s_%s_%s_", c.Key, c.From, c.To)
}
