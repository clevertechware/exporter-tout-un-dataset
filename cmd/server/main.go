package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	syncdemo "ctw/exporter-tout-un-dataset-example"
)

const seededRows = 10

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	container, err := postgres.Run(ctx, "postgres:18-alpine", postgres.BasicWaitStrategies())
	if err != nil {
		return fmt.Errorf("start postgres container: %w", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			log.Printf("terminate postgres container: %v", err)
		}
	}()

	connString, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("read connection string: %w", err)
	}
	if err = loadData(ctx, connString); err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return fmt.Errorf("open connection pool: %w", err)
	}
	defer pool.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	printUsage("http://" + listener.Addr().String())

	return serve(ctx, listener, syncdemo.ChangesHandler(pool))
}

func loadData(ctx context.Context, connString string) error {
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer conn.Close(ctx)

	if err = syncdemo.CreateSchema(ctx, conn); err != nil {
		return err
	}
	return syncdemo.SeedChanges(ctx, conn, seededRows)
}

func serve(ctx context.Context, listener net.Listener, handler http.Handler) error {
	mux := http.NewServeMux()
	mux.Handle("GET /changes", handler)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve http: %w", err)
	}
	return nil
}

func printUsage(baseURL string) {
	watermark := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Second)
	fmt.Printf("%d lignes chargées, une par minute sur les %d dernières minutes.\n\n", seededRows, seededRows)
	fmt.Println("Tout le dataset :")
	fmt.Printf("  curl -s %s/changes\n\n", baseURL)
	fmt.Println("Depuis un watermark (reculez-le d'une marge supérieure à la plus longue transaction) :")
	fmt.Printf("  curl -s -G %s/changes --data-urlencode 'updated_since=%s'\n\n",
		baseURL, watermark.Format(time.RFC3339))
	fmt.Println("Ctrl+C pour arrêter et supprimer le conteneur.")
}
