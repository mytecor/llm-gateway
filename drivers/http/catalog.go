package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mytecor/llm-gateway/driverproto"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

func listModels(ctx context.Context, msg inboundMessage) ([]driverproto.ModelInfo, error) {
	_, source, _, err := ValidateProviderConfig(msg)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, nil
	}
	return fetchCatalog(ctx, *source)
}

func fetchCatalog(ctx context.Context, source CatalogSource) ([]driverproto.ModelInfo, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, source.URL, nil)
	if err != nil {
		return nil, err
	}
	if source.APIKey != "" {
		request.Header.Set("Authorization", "Bearer "+source.APIKey)
	}
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("catalog returned HTTP %d", response.StatusCode)
	}
	var payload struct {
		Data []driverproto.ModelInfo `json:"data"`
	}

	decoder := json.NewDecoder(io.LimitReader(response.Body, 8<<20))
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode catalog: %w", err)
	}
	set := make(map[string]driverproto.ModelInfo, len(payload.Data))
	for _, item := range payload.Data {
		id := strings.TrimSpace(item.ID)
		if _, exists := set[id]; id != "" && !exists {
			item.ID = id
			set[id] = item
		}
	}
	if len(set) == 0 {
		return nil, errors.New("catalog contains no models")
	}
	models := make([]driverproto.ModelInfo, 0, len(set))
	for _, info := range set {
		models = append(models, info)
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	return models, nil
}
