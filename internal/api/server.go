package api

import (
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"hivemoji/internal/storage"
)

// Server exposes HTTP handlers for querying stored hivemoji data.
type Server struct {
	store *storage.Store
}

// New constructs the API server.
func New(store *storage.Store) *Server {
	return &Server{store: store}
}

// Register wires HTTP handlers onto an Echo instance.
func (s *Server) Register(e *echo.Echo) {
	e.GET("/health", s.handleHealth)
	e.GET("/api/emojis", s.handleList)
	e.GET("/api/authors/:author/emojis", s.handleListByAuthor)
	e.GET("/api/authors/:author/emojis/:name", s.handleGetByAuthor)
	e.GET("/api/emojis/:name", s.handleGet)
}

func (s *Server) handleHealth(c echo.Context) error {
	return c.String(http.StatusOK, "ok")
}

func (s *Server) handleList(c echo.Context) error {
	includeData := c.QueryParam("with_data") == "1" || strings.EqualFold(c.QueryParam("with_data"), "true")

	assets, err := s.store.ListAssets(c.Request().Context(), includeData)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	var resp []emojiResponse
	for _, a := range assets {
		resp = append(resp, toResponse(a, includeData))
	}

	return c.JSON(http.StatusOK, resp)
}

func (s *Server) handleListByAuthor(c echo.Context) error {
	author := c.Param("author")
	if strings.TrimSpace(author) == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "author is required")
	}

	includeData := c.QueryParam("with_data") == "1" || strings.EqualFold(c.QueryParam("with_data"), "true")

	assets, err := s.store.ListAssetsByAuthor(c.Request().Context(), author, includeData)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	var resp []emojiResponse
	for _, a := range assets {
		resp = append(resp, toResponse(a, includeData))
	}

	return c.JSON(http.StatusOK, resp)
}

func (s *Server) handleGet(c echo.Context) error {
	name := c.Param("name")
	if name == "" {
		return echo.ErrNotFound
	}

	author := c.QueryParam("author")
	if strings.TrimSpace(author) == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "author query param is required")
	}

	asset, err := s.store.GetAsset(c.Request().Context(), author, name)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if asset == nil {
		return echo.ErrNotFound
	}

	includeData := c.QueryParam("with_data") == "1" || strings.EqualFold(c.QueryParam("with_data"), "true")

	return c.JSON(http.StatusOK, toResponse(*asset, includeData))
}

func (s *Server) handleGetByAuthor(c echo.Context) error {
	author := c.Param("author")
	name := c.Param("name")
	if strings.TrimSpace(author) == "" || name == "" {
		return echo.ErrNotFound
	}

	includeData := c.QueryParam("with_data") == "1" || strings.EqualFold(c.QueryParam("with_data"), "true")

	asset, err := s.store.GetAsset(c.Request().Context(), author, name)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if asset == nil {
		return echo.ErrNotFound
	}
	return c.JSON(http.StatusOK, toResponse(*asset, includeData))
}

type emojiResponse struct {
	Name         string  `json:"name"`
	Version      int     `json:"version"`
	Author       *string `json:"author,omitempty"`
	UploadID     *string `json:"upload_id,omitempty"`
	Mime         string  `json:"mime"`
	Width        *int    `json:"width,omitempty"`
	Height       *int    `json:"height,omitempty"`
	Animated     bool    `json:"animated"`
	Loop         *int    `json:"loop,omitempty"`
	Checksum     *string `json:"checksum,omitempty"`
	FallbackMime *string `json:"fallback_mime,omitempty"`
	Data         string  `json:"data,omitempty"`
	FallbackData string  `json:"fallback_data,omitempty"`
}

func toResponse(asset storage.Asset, includeData bool) emojiResponse {
	resp := emojiResponse{
		Name:         asset.Name,
		Version:      asset.Version,
		Author:       asset.Author,
		UploadID:     asset.UploadID,
		Mime:         asset.Mime,
		Width:        asset.Width,
		Height:       asset.Height,
		Animated:     asset.Animated,
		Loop:         asset.Loop,
		Checksum:     asset.Checksum,
		FallbackMime: asset.FallbackMime,
	}

	if includeData {
		resp.Data = base64.StdEncoding.EncodeToString(asset.Data)
		if len(asset.FallbackData) > 0 {
			resp.FallbackData = base64.StdEncoding.EncodeToString(asset.FallbackData)
		}
	}
	return resp
}
