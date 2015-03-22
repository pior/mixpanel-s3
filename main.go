package main

import (
	"fmt"
	"gopkg.in/alecthomas/kingpin.v1"
	"io/ioutil"
	"log"
	"os"
	"time"
)

var (
	yesterday = time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	from = kingpin.Flag("from", "Extract from this date").Short('f').Default(yesterday).String()
	to   = kingpin.Flag("to", "Extract to this date").Short('t').Default(yesterday).String()

	key    = kingpin.Flag("key", "Mixpanel api key").Short('k').PlaceHolder("XXXXXX").OverrideDefaultFromEnvar("MIXPANEL_API_KEY").Required().String()
	secret = kingpin.Flag("secret", "Mixpanel secret key").Short('s').PlaceHolder("XXXXXX").OverrideDefaultFromEnvar("MIXPANEL_SECRET_KEY").Required().String()

	bucket = kingpin.Flag("bucket", "S3 bucket name").Short('b').OverrideDefaultFromEnvar("S3_BUCKET").Required().String()
	prefix = kingpin.Flag("prefix", "S3 key prefix").Short('p').OverrideDefaultFromEnvar("S3_PREFIX").String()

	split = kingpin.Flag("split", "Split by event name").Bool()
)

func run() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}()

	fullprefix := fmt.Sprintf("%s%s_%s_%s/", *prefix, *key, *from, *to)
	s3loader := NewS3Loader(*bucket, fullprefix)
	err := s3loader.Init()
	kingpin.FatalIfError(err, "s3")

	tmpfilename := fmt.Sprintf("mixpanel_%s_%s_%s_", *key, *from, *to)
	tmpfile, err := ioutil.TempFile("", tmpfilename)
	kingpin.FatalIfError(err, "tempfile")
	defer tmpfile.Close()

	log.Printf("Download from Mixpanel (%s)\n", tmpfile.Name())
	m := &MixpanelAPI{ApiKey: *key, ApiSecret: *secret}
	err = m.RawEvents(tmpfile, *from, *to)
	kingpin.FatalIfError(err, "mixpanel")

	if *split {
		log.Printf("Splitting by events")
		events, err := splitEvents(tmpfile)
		kingpin.FatalIfError(err, "splitter")

		for _, event := range events {
			log.Printf("Compress and Upload to S3 (%s)\n", s3loader.GetUrlForKey(event.Key+".gz"))
			err = s3loader.EmitGzip(event.file, event.Key+".gz")
			kingpin.FatalIfError(err, "upload")
		}
	} else {
		log.Printf("Compress and Upload to S3 (%s)\n", s3loader.GetUrlForKey("raw.gz"))
		err = s3loader.EmitGzip(tmpfile, "raw.gz")
		kingpin.FatalIfError(err, "upload")
	}
}

func main() {
	kingpin.CommandLine.Help = "Extract Mixpanel raw events and upload to S3"
	kingpin.Version("0.0.1")
	kingpin.Parse()
	run()
}
