package mixpanels3

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

func eventsUpload(l *S3Loader, eventBufs []*EventBuffer) error {
	tasks := make(chan *EventBuffer)
	var wg sync.WaitGroup
	var errCounter uint64 = 0

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			var err error
			for eb := range tasks {
				err = uploadFile(l, eb.File, eb.Key+".gz")
				if err != nil {
					atomic.AddUint64(&errCounter, 1)
				}
			}
			wg.Done()
		}()
	}

	for _, eb := range eventBufs {
		tasks <- eb
	}
	close(tasks)
	wg.Wait()

	if errCounter > 0 {
		return fmt.Errorf("%d files failed to upload", errCounter)
	}
	return nil
}

func uploadFile(l *S3Loader, f *os.File, key string) (err error) {
	log.Printf("Compress and upload to S3 (%s)\n", l.GetUrlForKey(key))

	err = l.EmitGzip(f, key)

	if err != nil {
		log.Printf("Failed to upload to S3 (%s): %s\n", l.GetUrlForKey(key), err)
	}

	return
}

func RunConfig(c *Config) (err error) {
	s3loader := NewS3Loader(c.Bucket, c.GetEffectiveS3Prefix())
	err = s3loader.Init()
	if err != nil {
		return fmt.Errorf("S3 failure: %s", err)
	}

	tmpfile, err := ioutil.TempFile("", c.GetTmpFilename())
	if err != nil {
		return fmt.Errorf("Tmp file failure: %s", err)
	}
	defer tmpfile.Close()

	log.Printf("Download from Mixpanel (%s)\n", tmpfile.Name())
	m := MixpanelAPI{ApiKey: c.Key, ApiSecret: c.Secret}
	err = m.RawEvents(tmpfile, c.From, c.To, c.Events)
	if err != nil {
		return fmt.Errorf("Mixpanel failure: %s", err)
	}

	if c.Split {
		log.Printf("Splitting by events")
		events, err := SplitEvents(tmpfile)
		if err != nil {
			return fmt.Errorf("Splitter failure: %s", err)
		}

		err = eventsUpload(s3loader, events)
	} else {
		err = uploadFile(s3loader, tmpfile, "raw.gz")
	}

	if err != nil {
		return fmt.Errorf("Upload failure: %s", err)
	}

	return
}
