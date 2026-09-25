# Démo : le trou de visibilité d'un curseur de synchro

Accompagne l'article *Exporter via API tout un dataset*. Reproduit contre un
vrai Postgres, via [testcontainers-go](https://golang.testcontainers.org/), le
scénario décrit dans l'article : une transaction longue (T1) insère une ligne
sans committer, une transaction courte (T2) insère et committe, un client de
synchro lit et avance son curseur, puis T1 committe enfin.

`TestNaiveCursorLosesRowFromTransactionCommittedAfterSync` lit avec un curseur
`position > $1` (l'équivalent séquence de `updated_at`) : la ligne de T1 n'est
plus jamais renvoyée, elle est perdue pour toujours.

## Prérequis

- Go 1.27+
- Docker (un daemon actif — Docker Desktop, Colima, etc.) : testcontainers-go
  démarre un vrai conteneur `postgres:18-alpine` pour le test.

## Commandes

```bash
go build ./...
go vet ./...
go test ./... -v
```

## Ce que montre cet exemple

Que la position dans une séquence (`BIGSERIAL`, et par extension `updated_at`)
n'est pas la position dans l'ordre des commits : un curseur qui avance sur cette
position peut sauter définitivement une ligne committée trop tard.
