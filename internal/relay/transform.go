package relay

import (
	"encoding/json"
	"fmt"
	"strings"

	"monitoring/internal/pubsub"
)

type sourcePayload struct {
	UUID          string             `json:"uuid"`
	Label         string             `json:"label"`
	Profile       profilePayload     `json:"profile"`
	EventSeverity string             `json:"event_severity"`
	Transport     transportPayload   `json:"transport"`
	Resources     resourcePayload    `json:"resources"`
	Components    []componentPayload `json:"components"`
}

type profilePayload struct {
	Label string `json:"label"`
}

type transportPayload struct {
	Bitrate int `json:"bitrate"`
}

type resourcePayload struct {
	UpTimeSec int `json:"up_time_sec"`
}

type componentPayload struct {
	ContentType string `json:"content_type"`
	Codec       string `json:"codec"`
	Resolution  string `json:"resolution"`
	Framerate   string `json:"framerate"`
	Bitrate     int    `json:"bitrate"`
}

type relayPayload struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	Status     string `json:"status"`
	Bitrate    string `json:"bitrate"`
	FrameRate  string `json:"frameRate"`
	Resolution string `json:"resolution"`
	AudioCodec string `json:"audioCodec"`
	VideoCodec string `json:"videoCodec"`
	Runtime    int    `json:"runtime"`
}

func buildRelayPayload(message pubsub.Message) ([]byte, error) {
	var src sourcePayload
	if len(message.Data) > 0 {
		if err := json.Unmarshal(message.Data, &src); err != nil {
			return nil, fmt.Errorf("unmarshal message data: %w", err)
		}
	}

	relay := relayPayload{
		ID:      firstNonEmpty(src.UUID, message.ID),
		Name:    src.Label,
		Type:    src.Profile.Label,
		Status:  normaliseStatus(src.EventSeverity),
		Runtime: src.Resources.UpTimeSec,
	}

	video := findComponent(src.Components, "video")
	audio := findComponent(src.Components, "audio")

	if video != nil {
		relay.VideoCodec = video.Codec
		relay.FrameRate = video.Framerate
		relay.Resolution = video.Resolution
		if video.Bitrate > 0 {
			relay.Bitrate = formatBitrate(video.Bitrate)
		}
	}

	if audio != nil {
		relay.AudioCodec = audio.Codec
		if relay.Bitrate == "" && audio.Bitrate > 0 {
			relay.Bitrate = formatBitrate(audio.Bitrate)
		}
	}

	if relay.Bitrate == "" && src.Transport.Bitrate > 0 {
		relay.Bitrate = formatBitrate(src.Transport.Bitrate)
	}

	return json.Marshal(relay)
}

func findComponent(components []componentPayload, contentType string) *componentPayload {
	contentType = strings.ToLower(contentType)
	for i := range components {
		if strings.ToLower(components[i].ContentType) == contentType {
			return &components[i]
		}
	}
	return nil
}

func formatBitrate(value int) string {
	if value <= 0 {
		return ""
	}
	return fmt.Sprintf("%d bps", value)
}

func normaliseStatus(severity string) string {
	s := strings.TrimSpace(strings.ToLower(severity))
	switch s {
	case "clear", "online", "ok", "normal":
		return "online"
	case "warning", "warn":
		return "warning"
	case "error", "critical", "alarm":
		return "error"
	case "offline", "down":
		return "offline"
	default:
		return s
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
