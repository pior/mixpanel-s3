package main

import (
	"fmt"
	"github.com/pior/mixpanels3"
	"gopkg.in/alecthomas/kingpin.v1"
	"io/ioutil"
	"log"
	"os"
	"sync"
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

func eventsUpload(l *mixpanels3.S3Loader, events []*mixpanels3.Event) {
	tasks := make(chan *mixpanels3.Event)
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			for e := range tasks {
				uploadFile(l, e.File, e.Key+".gz")
			}
			wg.Done()
		}()
	}

	for _, event := range events {
		tasks <- event
	}
	close(tasks)
	wg.Wait()
}

func uploadFile(l *mixpanels3.S3Loader, f *os.File, key string) {
	log.Printf("Compress and Upload to S3 (%s)\n", l.GetUrlForKey(key))
	err := l.EmitGzip(f, key)
	kingpin.FatalIfError(err, "upload")
}

func run() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	}()

	fullprefix := fmt.Sprintf("%s%s_%s_%s/", *prefix, *key, *from, *to)
	s3loader := mixpanels3.NewS3Loader(*bucket, fullprefix)
	err := s3loader.Init()
	kingpin.FatalIfError(err, "s3")

	tmpfilename := fmt.Sprintf("mixpanel_%s_%s_%s_", *key, *from, *to)
	tmpfile, err := ioutil.TempFile("", tmpfilename)
	kingpin.FatalIfError(err, "tempfile")
	defer tmpfile.Close()

	log.Printf("Download from Mixpanel (%s)\n", tmpfile.Name())
	m := mixpanels3.MixpanelAPI{ApiKey: *key, ApiSecret: *secret}
	err = m.RawEvents(tmpfile, *from, *to)
	kingpin.FatalIfError(err, "mixpanel")

	if *split {
		log.Printf("Splitting by events")
		events, err := mixpanels3.SplitEvents(tmpfile)
		kingpin.FatalIfError(err, "splitter")

		eventsUpload(s3loader, events)
	} else {
		uploadFile(s3loader, tmpfile, "raw.gz")
	}
}

func main() {
	kingpin.CommandLine.Help = "Extract Mixpanel raw events and upload to S3"
	kingpin.Version("0.0.1")
	kingpin.Parse()
	run()
}
