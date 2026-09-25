package syncdemo

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func startPostgres(t *testing.T) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("syncdemo"),
		postgres.WithUsername("syncdemo"),
		postgres.WithPassword("syncdemo"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate postgres container: %v", err)
		}
	})

	connString, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("read connection string: %v", err)
	}
	return connString
}

func connect(t *testing.T, ctx context.Context, connString string) *pgx.Conn {
	t.Helper()

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close connection: %v", err)
		}
	})
	return conn
}

// setupRaceScenario reproduit l'ordre du pipeline de l'article : T1 insère et
// reste ouverte, T2 insère et committe immédiatement, le client lit et avance
// son curseur, puis T1 committe enfin. renvoie la transaction T1 encore
// ouverte : à l'appelant de la committer pour terminer le scénario.
func setupRaceScenario(t *testing.T, ctx context.Context, admin *pgx.Conn, t1Conn, t2Conn *pgx.Conn) pgx.Tx {
	t.Helper()

	if err := createSchema(ctx, admin); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	t1, err := insertRow(ctx, t1Conn, "row-from-t1-still-open")
	if err != nil {
		t.Fatalf("start t1: %v", err)
	}

	t2, err := insertRow(ctx, t2Conn, "row-from-t2-committed")
	if err != nil {
		t.Fatalf("start t2: %v", err)
	}
	if err := t2.Commit(ctx); err != nil {
		t.Fatalf("commit t2: %v", err)
	}

	return t1
}

func TestNaiveCursorLosesRowFromTransactionCommittedAfterSync(t *testing.T) {
	ctx := context.Background()
	connString := startPostgres(t)

	admin := connect(t, ctx, connString)
	t1Conn := connect(t, ctx, connString)
	t2Conn := connect(t, ctx, connString)
	clientConn := connect(t, ctx, connString)

	t1 := setupRaceScenario(t, ctx, admin, t1Conn, t2Conn)

	firstSync, err := readSinceNaiveCursor(ctx, clientConn, 0, 10)
	if err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if len(firstSync) != 1 || firstSync[0].Payload != "row-from-t2-committed" {
		t.Fatalf("expected to see only t2's committed row, got %+v", firstSync)
	}
	cursor := firstSync[len(firstSync)-1].Position

	if err := t1.Commit(ctx); err != nil {
		t.Fatalf("commit t1: %v", err)
	}

	secondSync, err := readSinceNaiveCursor(ctx, clientConn, cursor, 10)
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	for _, row := range secondSync {
		if row.Payload == "row-from-t1-still-open" {
			t.Fatalf("t1's row should never reappear with a naive cursor, got %+v", secondSync)
		}
	}
}
