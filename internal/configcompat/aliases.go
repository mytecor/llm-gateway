// Package configcompat normalizes legacy JSON spellings before strict decoding.
package configcompat

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Aliases removes alternative spellings and produces only canonical keys.
// Conflicting values are rejected instead of depending on decoding order.
func Aliases(data []byte, aliases map[string][]string) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for canonical, names := range aliases {
		for _, name := range names {
			value, ok := fields[name]
			if !ok {
				continue
			}
			if existing, ok := fields[canonical]; ok {
				var a, b any
				if err := json.Unmarshal(existing, &a); err != nil {
					return nil, err
				}
				if err := json.Unmarshal(value, &b); err != nil {
					return nil, err
				}
				x, _ := json.Marshal(a)
				y, _ := json.Marshal(b)
				if !bytes.Equal(x, y) {
					return nil, fmt.Errorf("conflicting settings %q and %q", canonical, name)
				}
			} else {
				fields[canonical] = value
			}
			delete(fields, name)
		}
	}
	return json.Marshal(fields)
}
