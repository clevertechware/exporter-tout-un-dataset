package syncdemo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

// Change est une ligne exposée par l'API de synchro, avec l'horodatage de début de la transaction qui l'a écrite.
type Change struct {
	Position  int64     `json:"position"`
	Payload   string    `json:"payload"`
	UpdatedAt time.Time `json:"updated_at"`
}

type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func readUpdatedSince(ctx context.Context, db queryer, since time.Time) ([]Change, error) {
	rows, err := db.Query(ctx, `
		SELECT position, payload, updated_at
		FROM changes
		WHERE updated_at > $1
		ORDER BY updated_at, position
	`, since)
	if err != nil {
		return nil, fmt.Errorf("query changes updated since %s: %w", since, err)
	}
	defer rows.Close()

	var result []Change
	for rows.Next() {
		var c Change
		if err := rows.Scan(&c.Position, &c.Payload, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan change: %w", err)
		}
		result = append(result, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate changes: %w", err)
	}
	return result, nil
}

// ChangesHandler expose GET /changes?updated_since=<RFC 3339>. Sans paramètre, il renvoie tout le dataset.
func ChangesHandler(db queryer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var since time.Time
		if raw := r.URL.Query().Get("updated_since"); raw != "" {
			parsed, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				http.Error(w, "updated_since must be an RFC 3339 timestamp", http.StatusBadRequest)
				return
			}
			since = parsed
		}

		changes, err := readUpdatedSince(r.Context(), db, since)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(changes); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}
