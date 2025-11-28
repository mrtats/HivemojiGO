package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps DB access for hivemoji data.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore constructs a Store from a pgx pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// EnsureSchema creates tables used by the service.
func (s *Store) EnsureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS sync_state (
            key text PRIMARY KEY,
            value text NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now()
        )`,
		`CREATE TABLE IF NOT EXISTS hivemoji_assets (
            name text PRIMARY KEY,
            version int NOT NULL,
            author text,
            upload_id text,
            mime text NOT NULL,
            width int,
            height int,
            data bytea NOT NULL,
            animated boolean DEFAULT false,
            loop int,
            fallback_mime text,
            fallback_data bytea,
            checksum text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )`,
		`CREATE TABLE IF NOT EXISTS hivemoji_chunks (
            upload_id text NOT NULL,
            kind text NOT NULL,
            seq int NOT NULL,
            total int NOT NULL,
            data bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (upload_id, kind, seq)
        )`,
		`CREATE TABLE IF NOT EXISTS hivemoji_chunk_sets (
            upload_id text NOT NULL,
            kind text NOT NULL,
            name text NOT NULL,
            author text,
            version int NOT NULL,
            mime text NOT NULL,
            width int,
            height int,
            animated boolean,
            loop int,
            checksum text,
            total int NOT NULL,
            completed boolean NOT NULL DEFAULT false,
            data bytea,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (upload_id, kind)
        )`,
	}

	for _, stmt := range stmts {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}

	alters := []string{
		`ALTER TABLE hivemoji_assets ADD COLUMN IF NOT EXISTS author text`,
		`ALTER TABLE hivemoji_chunk_sets ADD COLUMN IF NOT EXISTS author text`,
		`UPDATE hivemoji_assets SET author = COALESCE(author, '')`,
		`UPDATE hivemoji_chunk_sets SET author = COALESCE(author, '')`,
		`ALTER TABLE hivemoji_assets DROP CONSTRAINT IF EXISTS hivemoji_assets_pkey`,
		`ALTER TABLE hivemoji_assets ADD CONSTRAINT hivemoji_assets_pkey PRIMARY KEY (author, name)`,
	}

	for _, stmt := range alters {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}

	return nil
}

// RegisterV1 represents a protocol v1 register payload after decoding.
type RegisterV1 struct {
	Name         string
	Author       string
	Mime         string
	Width        int
	Height       int
	Data         []byte
	Animated     bool
	Loop         *int
	FallbackMime string
	FallbackData []byte
}

// ChunkPayload captures a v2 chunk message after decoding.
type ChunkPayload struct {
	ID       string
	Author   string
	Name     string
	Version  int
	Mime     string
	Width    int
	Height   int
	Animated bool
	Loop     *int
	Checksum string
	Kind     string // main | fallback
	Seq      int
	Total    int
	Data     []byte
}

// AssembledSet represents a completed set of chunks.
type AssembledSet struct {
	UploadID string
	Kind     string
	Name     string
	Author   string
	Version  int
	Mime     string
	Width    int
	Height   int
	Animated bool
	Loop     *int
	Checksum string
	Data     []byte
}

// UpsertV1 stores or replaces an emoji registered via protocol v1.
func (s *Store) UpsertV1(ctx context.Context, payload RegisterV1) error {
	_, err := s.pool.Exec(ctx, `
        INSERT INTO hivemoji_assets (name, version, author, upload_id, mime, width, height, data, animated, loop, fallback_mime, fallback_data, checksum, updated_at)
        VALUES ($1, 1, $2, NULL, $3, $4, $5, $6, $7, $8, $9, $10, NULL, now())
        ON CONFLICT (author, name) DO UPDATE SET
            version = EXCLUDED.version,
            author = EXCLUDED.author,
            upload_id = EXCLUDED.upload_id,
            mime = EXCLUDED.mime,
            width = EXCLUDED.width,
            height = EXCLUDED.height,
            data = EXCLUDED.data,
            animated = EXCLUDED.animated,
            loop = EXCLUDED.loop,
            fallback_mime = EXCLUDED.fallback_mime,
            fallback_data = EXCLUDED.fallback_data,
            updated_at = now()
    `, payload.Name, payload.Author, payload.Mime, payload.Width, payload.Height, payload.Data, payload.Animated, payload.Loop, nullIfEmpty(payload.FallbackMime), nullBytes(payload.FallbackData))
	return err
}

// DeleteEmoji deletes a stored emoji by name.
func (s *Store) DeleteEmoji(ctx context.Context, author, name string) error {
	if strings.TrimSpace(author) == "" {
		return errors.New("author is required for delete")
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM hivemoji_assets WHERE author = $1 AND name = $2`, author, name)
	return err
}

// SaveChunk records a chunk and assembles the set when complete. It returns the completed set if this call closed it.
func (s *Store) SaveChunk(ctx context.Context, chunk ChunkPayload) (*AssembledSet, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Upsert chunk set metadata (without data until complete).
	_, err = tx.Exec(ctx, `
        INSERT INTO hivemoji_chunk_sets (upload_id, kind, name, author, version, mime, width, height, animated, loop, checksum, total, completed)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,false)
        ON CONFLICT (upload_id, kind) DO UPDATE SET
            name = EXCLUDED.name,
            author = EXCLUDED.author,
            version = EXCLUDED.version,
            mime = EXCLUDED.mime,
            width = EXCLUDED.width,
            height = EXCLUDED.height,
            animated = EXCLUDED.animated,
            loop = EXCLUDED.loop,
            checksum = EXCLUDED.checksum,
            total = EXCLUDED.total,
            updated_at = now()
    `, chunk.ID, chunk.Kind, chunk.Name, chunk.Author, chunk.Version, chunk.Mime, chunk.Width, chunk.Height, chunk.Animated, chunk.Loop, chunk.Checksum, chunk.Total)
	if err != nil {
		return nil, fmt.Errorf("upsert chunk set: %w", err)
	}

	// Insert chunk if not already present.
	_, err = tx.Exec(ctx, `
        INSERT INTO hivemoji_chunks (upload_id, kind, seq, total, data)
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (upload_id, kind, seq) DO NOTHING
    `, chunk.ID, chunk.Kind, chunk.Seq, chunk.Total, chunk.Data)
	if err != nil {
		return nil, fmt.Errorf("insert chunk: %w", err)
	}

	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM hivemoji_chunks WHERE upload_id=$1 AND kind=$2`, chunk.ID, chunk.Kind).Scan(&count); err != nil {
		return nil, fmt.Errorf("count chunks: %w", err)
	}

	if count < chunk.Total {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}

	assembled, err := s.assembleChunks(ctx, tx, chunk.ID, chunk.Kind)
	if err != nil {
		return nil, fmt.Errorf("assemble chunks: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return assembled, nil
}

// assembleChunks concatenates ordered chunks and marks the set complete.
func (s *Store) assembleChunks(ctx context.Context, tx pgx.Tx, uploadID, kind string) (*AssembledSet, error) {
	rows, err := tx.Query(ctx, `
        SELECT seq, data FROM hivemoji_chunks WHERE upload_id=$1 AND kind=$2 ORDER BY seq
    `, uploadID, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var parts [][]byte
	for rows.Next() {
		var seq int
		var data []byte
		if err := rows.Scan(&seq, &data); err != nil {
			return nil, err
		}
		parts = append(parts, data)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(parts) == 0 {
		return nil, errors.New("no chunks to assemble")
	}

	// Fetch metadata.
	var set AssembledSet
	var expectedTotal int
	err = tx.QueryRow(ctx, `
        SELECT upload_id, kind, name, author, version, mime, width, height, animated, loop, checksum, total
        FROM hivemoji_chunk_sets
        WHERE upload_id=$1 AND kind=$2
    `, uploadID, kind).Scan(&set.UploadID, &set.Kind, &set.Name, &set.Author, &set.Version, &set.Mime, &set.Width, &set.Height, &set.Animated, &set.Loop, &set.Checksum, &expectedTotal)
	if err != nil {
		return nil, err
	}

	if len(parts) != expectedTotal {
		return nil, fmt.Errorf("chunk count mismatch for %s/%s: have %d want %d", uploadID, kind, len(parts), expectedTotal)
	}

	var buf []byte
	for _, part := range parts {
		buf = append(buf, part...)
	}
	set.Data = buf

	if set.Checksum != "" {
		hash := sha256.Sum256(buf)
		if !strings.EqualFold(set.Checksum, hex.EncodeToString(hash[:])) {
			return nil, fmt.Errorf("checksum mismatch for upload %s kind %s", uploadID, kind)
		}
	}

	_, err = tx.Exec(ctx, `
        UPDATE hivemoji_chunk_sets SET data=$3, completed=true, updated_at=now() WHERE upload_id=$1 AND kind=$2
    `, uploadID, kind, buf)
	if err != nil {
		return nil, err
	}

	return &set, nil
}

// UpsertFromChunks saves an assembled set (and optional fallback) into the assets table.
func (s *Store) UpsertFromChunks(ctx context.Context, main *AssembledSet, fallback *AssembledSet) error {
	if main == nil {
		return errors.New("main set is required")
	}

	_, err := s.pool.Exec(ctx, `
        INSERT INTO hivemoji_assets (name, version, author, upload_id, mime, width, height, data, animated, loop, fallback_mime, fallback_data, checksum, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, now())
        ON CONFLICT (author, name) DO UPDATE SET
            version = EXCLUDED.version,
            author = EXCLUDED.author,
            upload_id = EXCLUDED.upload_id,
            mime = EXCLUDED.mime,
            width = EXCLUDED.width,
            height = EXCLUDED.height,
            data = EXCLUDED.data,
            animated = EXCLUDED.animated,
            loop = EXCLUDED.loop,
            fallback_mime = EXCLUDED.fallback_mime,
            fallback_data = EXCLUDED.fallback_data,
            checksum = EXCLUDED.checksum,
            updated_at = now()
    `, main.Name, main.Version, main.Author, main.UploadID, main.Mime, main.Width, main.Height, main.Data, main.Animated, main.Loop, fallbackMime(fallback), fallbackData(fallback), main.Checksum)
	return err
}

// GetChunkSet returns a completed chunk set if available.
func (s *Store) GetChunkSet(ctx context.Context, uploadID, kind string) (*AssembledSet, error) {
	row := s.pool.QueryRow(ctx, `
        SELECT upload_id, kind, name, author, version, mime, width, height, animated, loop, checksum, data
        FROM hivemoji_chunk_sets
        WHERE upload_id=$1 AND kind=$2 AND completed=true
    `, uploadID, kind)

	var set AssembledSet
	if err := row.Scan(&set.UploadID, &set.Kind, &set.Name, &set.Author, &set.Version, &set.Mime, &set.Width, &set.Height, &set.Animated, &set.Loop, &set.Checksum, &set.Data); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &set, nil
}

// SetLastBlock stores the last processed block number.
func (s *Store) SetLastBlock(ctx context.Context, number int64) error {
	_, err := s.pool.Exec(ctx, `
        INSERT INTO sync_state (key, value, updated_at)
        VALUES ('last_block', $1, now())
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
    `, fmt.Sprintf("%d", number))
	return err
}

// LastBlock returns the last processed block number if present.
func (s *Store) LastBlock(ctx context.Context) (int64, error) {
	var value string
	err := s.pool.QueryRow(ctx, `SELECT value FROM sync_state WHERE key='last_block'`).Scan(&value)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	var n int64
	_, err = fmt.Sscan(value, &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// FetchAsset returns a stored emoji asset.
type Asset struct {
	Name         string
	Version      int
	Author       *string
	UploadID     *string
	Mime         string
	Width        *int
	Height       *int
	Animated     bool
	Loop         *int
	Checksum     *string
	FallbackMime *string
	Data         []byte
	FallbackData []byte
}

// GetAsset retrieves an emoji by author and name.
func (s *Store) GetAsset(ctx context.Context, author, name string) (*Asset, error) {
	row := s.pool.QueryRow(ctx, `
        SELECT name, version, author, upload_id, mime, width, height, animated, loop, checksum, fallback_mime, data, fallback_data
        FROM hivemoji_assets WHERE author=$1 AND name=$2
    `, author, name)

	var asset Asset
	var uploadID *string
	var authorPtr *string
	var width *int
	var height *int
	var loop *int
	var checksum *string
	var fallbackMime *string
	var data []byte
	var fallbackData []byte

	if err := row.Scan(&asset.Name, &asset.Version, &authorPtr, &uploadID, &asset.Mime, &width, &height, &asset.Animated, &loop, &checksum, &fallbackMime, &data, &fallbackData); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	asset.UploadID = uploadID
	asset.Author = authorPtr
	asset.Width = width
	asset.Height = height
	asset.Loop = loop
	asset.Checksum = checksum
	asset.FallbackMime = fallbackMime
	asset.Data = data
	asset.FallbackData = fallbackData
	return &asset, nil
}

// ListAssets fetches all stored emoji metadata (without binary payloads unless requested).
func (s *Store) ListAssets(ctx context.Context, includeData bool) ([]Asset, error) {
	cols := "name, version, author, upload_id, mime, width, height, animated, loop, checksum, fallback_mime"
	if includeData {
		cols += ", data, fallback_data"
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf("SELECT %s FROM hivemoji_assets ORDER BY name", cols))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var assets []Asset
	for rows.Next() {
		var asset Asset
		if includeData {
			var uploadID *string
			var author *string
			var width *int
			var height *int
			var loop *int
			var checksum *string
			var fallbackMime *string
			var data []byte
			var fallbackData []byte

			if err := rows.Scan(&asset.Name, &asset.Version, &author, &uploadID, &asset.Mime, &width, &height, &asset.Animated, &loop, &checksum, &fallbackMime, &data, &fallbackData); err != nil {
				return nil, err
			}
			asset.UploadID = uploadID
			asset.Author = author
			asset.Width = width
			asset.Height = height
			asset.Loop = loop
			asset.Checksum = checksum
			asset.FallbackMime = fallbackMime
			asset.Data = data
			asset.FallbackData = fallbackData
		} else {
			var uploadID *string
			var author *string
			var width *int
			var height *int
			var loop *int
			var checksum *string
			var fallbackMime *string

			if err := rows.Scan(&asset.Name, &asset.Version, &author, &uploadID, &asset.Mime, &width, &height, &asset.Animated, &loop, &checksum, &fallbackMime); err != nil {
				return nil, err
			}
			asset.UploadID = uploadID
			asset.Author = author
			asset.Width = width
			asset.Height = height
			asset.Loop = loop
			asset.Checksum = checksum
			asset.FallbackMime = fallbackMime
		}
		assets = append(assets, asset)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return assets, nil
}

// ListAssetsByAuthor fetches emojis for a specific author.
func (s *Store) ListAssetsByAuthor(ctx context.Context, author string, includeData bool) ([]Asset, error) {
	if strings.TrimSpace(author) == "" {
		return nil, errors.New("author is required")
	}

	cols := "name, version, author, upload_id, mime, width, height, animated, loop, checksum, fallback_mime"
	if includeData {
		cols += ", data, fallback_data"
	}

	rows, err := s.pool.Query(ctx, fmt.Sprintf("SELECT %s FROM hivemoji_assets WHERE author=$1 ORDER BY name", cols), author)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var assets []Asset
	for rows.Next() {
		var asset Asset
		if includeData {
			var uploadID *string
			var auth *string
			var width *int
			var height *int
			var loop *int
			var checksum *string
			var fallbackMime *string
			var data []byte
			var fallbackData []byte

			if err := rows.Scan(&asset.Name, &asset.Version, &auth, &uploadID, &asset.Mime, &width, &height, &asset.Animated, &loop, &checksum, &fallbackMime, &data, &fallbackData); err != nil {
				return nil, err
			}
			asset.Author = auth
			asset.UploadID = uploadID
			asset.Width = width
			asset.Height = height
			asset.Loop = loop
			asset.Checksum = checksum
			asset.FallbackMime = fallbackMime
			asset.Data = data
			asset.FallbackData = fallbackData
		} else {
			var uploadID *string
			var auth *string
			var width *int
			var height *int
			var loop *int
			var checksum *string
			var fallbackMime *string

			if err := rows.Scan(&asset.Name, &asset.Version, &auth, &uploadID, &asset.Mime, &width, &height, &asset.Animated, &loop, &checksum, &fallbackMime); err != nil {
				return nil, err
			}
			asset.Author = auth
			asset.UploadID = uploadID
			asset.Width = width
			asset.Height = height
			asset.Loop = loop
			asset.Checksum = checksum
			asset.FallbackMime = fallbackMime
		}
		assets = append(assets, asset)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return assets, nil
}

func nullIfEmpty(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return &value
}

func nullBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

func fallbackMime(set *AssembledSet) *string {
	if set == nil {
		return nil
	}
	return &set.Mime
}

func fallbackData(set *AssembledSet) []byte {
	if set == nil {
		return nil
	}
	return set.Data
}
