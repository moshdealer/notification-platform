package utils

import (
	"encoding/json"
	"fmt"
	"strings"
)

// TODO Потом убрать вообще этот модуль

func ExtractEventID(data []byte) uint {
	type outer struct {
		EventID uint `json:"event_id"`
	}

	var o outer
	if err := json.Unmarshal(data, &o); err != nil {
		fmt.Printf("ERROR: failed to parse event_id. Raw: %s\n", string(data))
		return 0
	}

	if o.EventID != 0 {
		return o.EventID
	}

	fmt.Printf("WARNING: event_id not found or zero in message: %s\n", string(data))
	return 0
}

// ExtractPriority возвращает priority из сообщения NATS
func ExtractPriority(data []byte) string {
	type outer struct {
		Payload struct {
			Priority string `json:"priority"`
		} `json:"payload"`
	}

	var o outer

	if err := json.Unmarshal(data, &o); err != nil {
		fmt.Printf("ERROR: failed to parse priority. Raw: %s\n", string(data))
		return ""
	}

	if o.Payload.Priority != "" {
		return strings.ToLower(strings.TrimSpace(o.Payload.Priority))
	}

	// Если priority не нашли в payload - попробуем на верхнем уровне (на всякий случай)
	type fallback struct {
		Priority string `json:"priority"`
	}
	var fb fallback
	if err := json.Unmarshal(data, &fb); err == nil && fb.Priority != "" {
		return strings.ToLower(strings.TrimSpace(fb.Priority))
	}

	fmt.Printf("WARNING: priority not found in message: %s\n", string(data))
	return ""
}
