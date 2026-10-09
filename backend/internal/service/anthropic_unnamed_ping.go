package service

import (
	"encoding/json"
	"strings"
)

type anthropicPingState struct {
	eventNamed bool
	dataSeen   bool
	pending    string
}

// next repairs only a complete, single-data-line heartbeat. At most that one
// candidate line is held until the frame boundary; unknown or multiline frames
// stay byte-for-byte unchanged for downstream protocol validation.
func (s anthropicPingState) next(line string) (wire string, next anthropicPingState, repaired bool) {
	next = s
	wire = line + "\n"
	if line == "" {
		next.eventNamed = false
		next.dataSeen = false
	} else if value, ok := strings.CutPrefix(line, "event:"); ok {
		next.eventNamed = strings.TrimSpace(value) != ""
	} else if strings.HasPrefix(line, "data:") {
		next.dataSeen = true
	}
	if s.pending != "" {
		next.pending = ""
		wire = s.pending + "\n" + wire
		if line == "" {
			wire = "event: ping\n" + wire
			repaired = true
		}
		return wire, next, repaired
	}
	if !s.eventNamed && !s.dataSeen && isAnthropicPingDataLine(line) {
		next.pending = line
		return "", next, false
	}
	return wire, next, false
}

func isAnthropicPingDataLine(line string) bool {
	data, ok := extractAnthropicSSEDataLine(line)
	if !ok {
		return false
	}
	var payload map[string]json.RawMessage
	if json.Unmarshal([]byte(data), &payload) != nil || len(payload) != 1 {
		return false
	}
	var kind string
	return json.Unmarshal(payload["type"], &kind) == nil && kind == "ping"
}
