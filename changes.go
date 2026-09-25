// Package syncdemo reproduit le trou de visibilité décrit dans l'article :
// un curseur naïf sur une colonne séquentielle peut perdre définitivement une
// ligne dont la transaction committe après qu'un client de synchro est passé.
package syncdemo

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const schema = `
CREATE TABLE changes (
    position BIGSERIAL PRIMARY KEY,
    payload  TEXT NOT NULL
);
`

// Row est une ligne du journal des changements telle que renvoyée au client de synchro.
type Row struct {
	Position int64
	Payload  string
}

func createSchema(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	return nil
}

// insertRow ouvre une transaction, insère une ligne et laisse l'appelant
// décider du moment du commit (ou du rollback).
func insertRow(ctx context.Context, conn *pgx.Conn, payload string) (pgx.Tx, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO changes (payload) VALUES ($1)", payload); err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("insert row: %w", err)
	}
	return tx, nil
}

// readSinceNaiveCursor est le curseur naïf de l'article : trié sur la clé
// séquentielle. C'est celui qui perd la ligne
// d'une transaction encore ouverte au moment de la lecture.
func readSinceNaiveCursor(ctx context.Context, conn *pgx.Conn, lastPosition int64, limit int) ([]Row, error) {
	rows, err := conn.Query(ctx, `
		SELECT position, payload
		FROM changes
		WHERE position > $1
		ORDER BY position
		LIMIT $2
	`, lastPosition, limit)
	if err != nil {
		return nil, fmt.Errorf("query changes with naive cursor: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

func scanRows(rows pgx.Rows) ([]Row, error) {
	var result []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.Position, &r.Payload); err != nil {
			return nil, fmt.Errorf("scan changes row: %w", err)
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate changes rows: %w", err)
	}
	return result, nil
}
