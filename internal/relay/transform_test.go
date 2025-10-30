package relay

import (
	"encoding/json"
	"testing"
	"time"

	"monitoring/internal/pubsub"
)

func TestBuildRelayPayload(t *testing.T) {
	input := []byte(`{
	  "uuid": "449149c3-d7d9-43dc-851c-a29c6badfced",
	  "label": "kwtx-source",
	  "event_severity": "Clear",
	  "profile": { "label": "Default" },
	  "transport": { "bitrate": 6563459 },
	  "resources": { "up_time_sec": 22682 },
	  "components": [
	    { "content_type": "Video", "codec": "H.264", "resolution": "1280x720p", "framerate": "59.94", "bitrate": 5998625 },
	    { "content_type": "Audio", "codec": "ADTS/AAC", "bitrate": 135988 }
	  ]
	}`)

	msg := pubsub.Message{
		ID:          "message-id",
		Data:        input,
		Attributes:  map[string]string{"foo": "bar"},
		PublishTime: time.Unix(0, 0),
	}

	payload, err := buildRelayPayload(msg)
	if err != nil {
		t.Fatalf("buildRelayPayload returned error: %v", err)
	}

	var got relayPayload
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}

	expected := relayPayload{
		ID:         "449149c3-d7d9-43dc-851c-a29c6badfced",
		Name:       "kwtx-source",
		Type:       "Default",
		Status:     "online",
		Bitrate:    "5998625 bps",
		FrameRate:  "59.94",
		Resolution: "1280x720p",
		AudioCodec: "ADTS/AAC",
		VideoCodec: "H.264",
		Runtime:    22682,
	}

	if diff := diffRelayPayload(expected, got); diff != "" {
		t.Fatalf("unexpected relay payload diff:\n%s", diff)
	}
}

func TestBuildRelayPayloadFallbacks(t *testing.T) {
	msg := pubsub.Message{
		ID:   "fallback-id",
		Data: []byte(`{"components": []}`),
	}

	payload, err := buildRelayPayload(msg)
	if err != nil {
		t.Fatalf("buildRelayPayload returned error: %v", err)
	}

	var got relayPayload
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}

	if got.ID != "fallback-id" {
		t.Fatalf("expected ID fallback-id, got %q", got.ID)
	}

	if got.Status != "" {
		t.Fatalf("expected empty status, got %q", got.Status)
	}
}

func TestNormaliseStatus(t *testing.T) {
	testCases := map[string]string{
		"Clear":   "online",
		" warning": "warning",
		"ERROR":   "error",
		"Offline": "offline",
		"unknown": "unknown",
		"":        "",
	}

	for input, expected := range testCases {
		if got := normaliseStatus(input); got != expected {
			t.Fatalf("normaliseStatus(%q) = %q, expected %q", input, got, expected)
		}
	}
}

func TestFormatBitrate(t *testing.T) {
	if got := formatBitrate(0); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}

	if got := formatBitrate(1024); got != "1024 bps" {
		t.Fatalf("expected formatted bitrate, got %q", got)
	}
}

func TestFirstNonEmpty(t *testing.T) {
	if got := firstNonEmpty("", " ", "value", "other"); got != "value" {
		t.Fatalf("expected first non-empty value, got %q", got)
	}

	if got := firstNonEmpty("", " "); got != "" {
		t.Fatalf("expected empty string when none provided, got %q", got)
	}
}

func TestFindComponent(t *testing.T) {
	components := []componentPayload{{ContentType: "Audio"}, {ContentType: "Video"}}

	if comp := findComponent(components, "video"); comp == nil || comp.ContentType != "Video" {
		t.Fatalf("expected to find video component")
	}

	if comp := findComponent(components, "data"); comp != nil {
		t.Fatalf("expected nil for unknown component")
	}
}

func diffRelayPayload(expected, actual relayPayload) string {
	if expected == actual {
		return ""
	}
	buf, _ := json.MarshalIndent(struct {
		Expected relayPayload `json:"expected"`
		Actual   relayPayload `json:"actual"`
	}{expected, actual}, "", "  ")
	return string(buf)
}
