package mixpanels3

import (
	"bufio"
	"encoding/json"
	"github.com/extemporalgenome/slug"
	"io"
	"io/ioutil"
	"log"
	"os"
)

type Event struct {
	Name   string
	Key    string
	Length int
	File   *os.File
	w      *bufio.Writer
}

func (e *Event) createTmpFile() {
	var err error
	e.File, err = ioutil.TempFile("", "mixpanels3-event-")
	if err != nil {
		panic(err)
	}
	e.w = bufio.NewWriter(e.File)
}

func newEvent(name string) *Event {
	e := Event{Name: name, Key: slug.Slug(name), Length: 0}
	e.createTmpFile()
	return &e
}

type EventPayload struct {
	Event string `json:"event"`
}

func SplitEvents(input io.Reader) (events []*Event, err error) {
	var payload EventPayload
	var eventsMap = make(map[string]*Event)

	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		raw_line := scanner.Bytes()
		err = json.Unmarshal(raw_line, &payload)
		if err != nil {
			log.Printf("Bad record: %v\n", err)
			continue
		}

		e, ok := eventsMap[payload.Event]
		if !ok {
			e = newEvent(payload.Event)
			eventsMap[payload.Event] = e
			events = append(events, e)
		}
		e.w.Write(raw_line)
		e.Length += 1
	}

	for _, e := range events {
		e.w.Flush()
		e.File.Seek(0, os.SEEK_SET)
	}

	err = scanner.Err()
	return
}
