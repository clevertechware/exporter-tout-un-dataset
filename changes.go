package syncdemo

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const schema = `
CREATE TABLE changes (
    position BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payload    TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

// Row est une ligne du journal des changements telle que renvoyée au client de synchro.
type Row struct {
	Position int64
	Payload  string
}

func CreateSchema(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	return nil
}

// SeedChanges insère count lignes espacées d'une minute, la plus récente datant d'il y a une minute.
func SeedChanges(ctx context.Context, conn *pgx.Conn, count int) error {
	_, err := conn.Exec(ctx, `
		INSERT INTO changes (payload, updated_at)
		SELECT 'record-' || i, now() - (($1::int - i + 1) * interval '1 minute')
		FROM generate_series(1, $1::int) AS i
	`, count)
	if err != nil {
		return fmt.Errorf("seed changes: %w", err)
	}
	return nil
}

// insertRow ouvre une transaction, insère une ligne et laisse l'appelant décider du moment du commit (ou du rollback).
func insertRow(ctx context.Context, conn *pgx.Conn, payload string) (pgx.Tx, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO changes (payload) VALUES ($1)", payload); err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("insert row: %w", err)
	}
	return tx, nil
}

// readSinceNaiveCursor est le curseur naïf de l'article : trié sur la clé séquentielle.
//
// C'est celui qui perd la ligne d'une transaction encore ouverte au moment de la lecture.
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
