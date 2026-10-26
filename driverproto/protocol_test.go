package driverproto

import (
	"bufio"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func checkFixtures[T any](t *testing.T, path string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scan := bufio.NewScanner(file)
	for scan.Scan() {
		raw := scan.Bytes()
		var value T
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatal(err)
		}
		actual, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var want, got any
		if err := json.Unmarshal(raw, &want); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(actual, &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("wire contract changed:\nwant %s\ngot  %s", raw, actual)
		}
	}
	if err := scan.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestProtocolFixtures(t *testing.T) {
	checkFixtures[Command[json.RawMessage]](t, "testdata/session-commands.ndjson")
	checkFixtures[Command[json.RawMessage]](t, "testdata/stateless-commands.ndjson")
	checkFixtures[Event](t, "testdata/events.ndjson")
}

func TestHandshakeRejectsUnknownVersionsAndMissingCapabilities(t *testing.T) {
	for _, tc := range []struct {
		event Event
		valid bool
	}{
		{Event{Type: "ready", ProtocolVersion: Version, Capabilities: &Capabilities{Chat: true}}, true},
		{Event{Type: "ready", Capabilities: &Capabilities{Chat: true}}, true},
		{Event{Type: "ready", ProtocolVersion: Version}, false},
		{Event{Type: "ready", ProtocolVersion: Version + 1, Capabilities: &Capabilities{}}, false},
		{Event{Type: "other", Capabilities: &Capabilities{}}, false},
	} {
		if err := ValidateReady(tc.event); (err == nil) != tc.valid {
			t.Fatalf("handshake %v: %v", tc.event, err)
		}
	}
}
