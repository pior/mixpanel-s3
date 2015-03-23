package mixpanels3

import (
	"bufio"
	"encoding/json"
	"github.com/extemporalgenome/slug"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"sync"
)

type Event struct {
	name    string
	payload []byte
}

type EventBuffer struct {
	Name string
	Key  string
	File *os.File
	w    *bufio.Writer
}

func (e *EventBuffer) createTmpFile() {
	var err error
	e.File, err = ioutil.TempFile("", "mixpanels3-event-"+e.Key+"-")
	if err != nil {
		panic(err)
	}
	e.w = bufio.NewWriter(e.File)
}

func newEvent(name string) *EventBuffer {
	e := EventBuffer{Name: name, Key: slug.Slug(name)}
	e.createTmpFile()
	return &e
}

type EventRecord struct {
	Event string `json:"event"`
}

func SplitEvents(input io.Reader) (events []*EventBuffer, err error) {
	var eventsMap = make(map[string]*EventBuffer)

	raw_lines := make(chan []byte, 100)
	output := make(chan *Event, 100)

	var taskWG sync.WaitGroup
	for i := 0; i < runtime.NumCPU(); i++ {
		taskWG.Add(1)
		go func() {
			var r EventRecord
			for raw_line := range raw_lines {
				err = json.Unmarshal(raw_line, &r)
				if err != nil {
					log.Printf("Bad record: %v\n", err)
					continue
				}
				output <- &Event{name: r.Event, payload: raw_line}
			}
			taskWG.Done()
		}()
	}

	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		for event := range output {
			e, ok := eventsMap[event.name]
			if !ok {
				e = newEvent(event.name)
				eventsMap[event.name] = e
				events = append(events, e)
			}
			e.w.Write(event.payload)
			e.w.WriteByte('\n')
		}
		writerWG.Done()
	}()

	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		a := scanner.Bytes()
		b := make([]byte, len(a))
		copy(b, a)
		raw_lines <- b
	}

	close(raw_lines)
	taskWG.Wait()

	close(output)
	writerWG.Wait()

	for _, e := range events {
		e.w.Flush()
		e.File.Seek(0, os.SEEK_SET)
	}

	err = scanner.Err()
	return
}
