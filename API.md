# Hivemoji API

Base URL: `http://localhost:8080` (adjust to your deployment, e.g. `https://api.example.com`).

## Health
`GET /health`
- Response: `200 OK`, body `ok`.

## List all emojis
`GET /api/emojis`
- Query: `with_data` (`1`/`true`, optional) to include base64 `data`/`fallback_data`.
- Response: `200 OK` array of emoji objects.

## List emojis by author
`GET /api/authors/{author}/emojis`
- Query: `with_data` (`1`/`true`, optional).
- Response: `200 OK` array of emoji objects.

## Get emoji by author/name (preferred)
`GET /api/authors/{author}/emojis/{name}`
- Query: `with_data` (`1`/`true`, optional).
- Response: `200 OK` emoji object.

## Get emoji (legacy path, requires author query)
`GET /api/emojis/{name}?author={author}`
- Query: `author` (required), `with_data` (`1`/`true`, optional).
- Response: `200 OK` emoji object.

## Emoji object fields
- `name` (string)
- `version` (int)
- `author` (string, omitted if empty)
- `upload_id` (string, v2 only, omitted if empty)
- `mime` (string)
- `width`, `height` (ints, omitted if null)
- `animated` (bool)
- `loop` (int, omitted if null)
- `checksum` (string, omitted if null)
- `fallback_mime` (string, omitted if null)
- `data` (base64 string, only when `with_data`)
- `fallback_data` (base64 string, only when present and `with_data`)

## Errors
- `400 Bad Request`: missing/invalid parameters.
- `404 Not Found`: emoji not found.
- `500 Internal Server Error`: server/db errors.

Notes:
- Names are unique per author; always specify author for lookups.
- Binary image data is base64-encoded when `with_data=1|true`.
- Accepted mime types: `image/png`, `image/webp`, `image/gif`. Other mime values are ignored during registration and will not be served.
