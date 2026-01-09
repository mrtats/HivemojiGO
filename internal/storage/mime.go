package storage

import (
	"mime"
	"strings"
)

var allowedEmojiMimes = map[string]struct{}{
	"image/gif":  {},
	"image/png":  {},
	"image/webp": {},
}

// NormalizeEmojiMime validates and normalizes emoji mime types to safe image formats.
func NormalizeEmojiMime(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}

	mediaType, _, err := mime.ParseMediaType(raw)
	if err != nil {
		return "", false
	}

	mediaType = strings.ToLower(mediaType)
	if _, ok := allowedEmojiMimes[mediaType]; !ok {
		return "", false
	}
	return mediaType, true
}
