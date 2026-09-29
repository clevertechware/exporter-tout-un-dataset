package syncdemo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// SyncClient synchronise les changements d'une API `updated_since`.
//
// Avant chaque appel il recule le watermark de `margin`, qui doit dépasser la durée de la plus longue transaction
// côté serveur. Les lignes revues sont absorbées par la clé `position` : la synchro est idempotente.
type SyncClient struct {
	baseURL   string
	margin    time.Duration
	watermark time.Time
	changes   map[int64]Change
}

func NewSyncClient(baseURL string, margin time.Duration) *SyncClient {
	return &SyncClient{baseURL: baseURL, margin: margin, changes: map[int64]Change{}}
}

func (c *SyncClient) Changes() map[int64]Change {
	return c.changes
}

func (c *SyncClient) Sync(ctx context.Context) error {
	fetched, err := c.fetch(ctx, c.sinceWithMargin())
	if err != nil {
		return err
	}
	for _, change := range fetched {
		c.changes[change.Position] = change
		if change.UpdatedAt.After(c.watermark) {
			c.watermark = change.UpdatedAt
		}
	}
	return nil
}

func (c *SyncClient) sinceWithMargin() time.Time {
	if c.watermark.IsZero() {
		return time.Time{}
	}
	return c.watermark.Add(-c.margin)
}

func (c *SyncClient) fetch(ctx context.Context, since time.Time) ([]Change, error) {
	endpoint := c.baseURL + "/changes"
	if !since.IsZero() {
		endpoint += "?updated_since=" + url.QueryEscape(since.Format(time.RFC3339Nano))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call changes api: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("call changes api: unexpected status %d", resp.StatusCode)
	}

	var changes []Change
	if err = json.NewDecoder(resp.Body).Decode(&changes); err != nil {
		return nil, fmt.Errorf("decode changes: %w", err)
	}
	return changes, nil
}
