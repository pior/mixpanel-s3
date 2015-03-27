package main

import (
	"fmt"
	"github.com/pior/mixpanels3"
	"gopkg.in/alecthomas/kingpin.v1"
	"os"
	"time"
)

var (
	yesterday = time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	from = kingpin.Flag("from", "Extract from this date").Short('f').
		Default(yesterday).String()
	to = kingpin.Flag("to", "Extract to this date").Short('t').
		Default(yesterday).String()
	event = kingpin.Flag("event", "Extract only this event").Short('e').
		Default("").String()

	key = kingpin.Flag("key", "Mixpanel api key").Short('k').
		PlaceHolder("XXXXXX").OverrideDefaultFromEnvar("MIXPANEL_API_KEY").
		Required().String()
	secret = kingpin.Flag("secret", "Mixpanel secret key").Short('s').
		PlaceHolder("XXXXXX").OverrideDefaultFromEnvar("MIXPANEL_SECRET_KEY").
		Required().String()

	bucket = kingpin.Flag("bucket", "S3 bucket name").Short('b').
		OverrideDefaultFromEnvar("S3_BUCKET").Required().String()
	prefix = kingpin.Flag("prefix", "S3 key prefix").Short('p').
		OverrideDefaultFromEnvar("S3_PREFIX").String()

	split = kingpin.Flag("split", "Split by event name").Bool()
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}()

	kingpin.CommandLine.Help = "Extract Mixpanel raw events and upload to S3"
	kingpin.Version("1.0")
	kingpin.Parse()

	err := mixpanels3.Run(*from, *to, *event, *key, *secret, *bucket, *prefix,
		*split)
	kingpin.FatalIfError(err, "")
}
