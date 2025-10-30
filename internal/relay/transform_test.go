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
