package main

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
	length int
	file   *os.File
	w      *bufio.Writer
}

func (e *Event) createTmpFile() {
	var err error
	e.file, err = ioutil.TempFile("", "mixpanels3-event-")
	if err != nil {
		panic(err)
	}
	e.w = bufio.NewWriter(e.file)
}

func newEvent(name string) *Event {
	e := Event{Name: name, Key: slug.Slug(name), length: 0}
	e.createTmpFile()
	return &e
}

type EventPayload struct {
	Event string `json:"event"`
}

func splitEvents(input io.Reader) (events []*Event, err error) {
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
		e.length += 1
	}

	for _, e := range events {
		// log.Printf("Flushing event buffer for %s\n", e.Name)
		e.w.Flush()
		e.file.Seek(0, os.SEEK_SET)
	}

	err = scanner.Err()
	return
}

// func main() {
// 	f, _ := os.Open("/tmp/mixpanel_7c10d76d2e0c21bf038780fa489b2fc1_2015-03-21_2015-03-21_887139523")

// 	events, _ := splitEvents(f)

// 	// c := 0
// 	for _, e := range events {
// 		log.Printf("Event %s has %d records", e.Key, e.length)
// 		// c += e.length
// 	}
// 	// log.Printf("Total: %d", c)
// }
