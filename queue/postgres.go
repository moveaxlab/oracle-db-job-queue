package queue

import (
	"context"
	"database/sql"
	"errors"
	"log"

	_ "github.com/lib/pq"
	"github.com/moveaxlab/go-optional"
)

type postgresQueue struct {
	db *sql.DB
}

const createPostgresTable = `
CREATE TABLE IF NOT EXISTS email_outbox (
	id SERIAL PRIMARY KEY,
	recipient TEXT NOT NULL,
	subject TEXT NOT NULL,
	body TEXT NOT NULL
)
`

func NewPostgresQueue() Queue {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@postgres:5432/test")
	if err != nil {
		log.Fatal(err)
	}
	return &postgresQueue{db: db}
}

func (q *postgresQueue) Count() int {
	row := q.db.QueryRow("SELECT COUNT(*) FROM email_outbox")
	if row.Err() != nil {
		log.Fatalf("failed to count rows: %v", row.Err())
	}
	var count int
	err := row.Scan(&count)
	if err != nil {
		log.Fatalf("failed to scan row count: %v", err)
	}
	return count
}

func (q *postgresQueue) Truncate() {
	_, err := q.db.Exec("TRUNCATE TABLE email_outbox")
	if err != nil {
		log.Fatalf("failed to truncate outbox table: %v", err)
	}
}

const postgresTxKey txKey = "postgres_tx"

func (q *postgresQueue) Begin(ctx context.Context) context.Context {
	tx, err := q.db.Begin()
	if err != nil {
		log.Fatalf("failed to start transaction: %v", err)
	}
	return context.WithValue(ctx, postgresTxKey, tx)
}

func (q *postgresQueue) Commit(ctx context.Context) {
	tx, ok := ctx.Value(postgresTxKey).(*sql.Tx)
	if !ok {
		log.Fatal("cannot commit outside of a transaction")
	}
	err := tx.Commit()
	if err != nil {
		log.Fatalf("failed to commit transaction: %v", err)
	}
}

func (q *postgresQueue) Migrate() {
	_, err := q.db.Exec(createPostgresTable)
	if err != nil {
		log.Fatalf("failed to create tables: %v", err)
	}
}

func (q *postgresQueue) Enqueue(ctx context.Context, email Email) {
	_, err := q.db.Exec("INSERT INTO email_outbox (recipient, subject, body) VALUES ($1, $2, $3)", email.Recipient, email.Subject, email.Body)
	if err != nil {
		log.Fatalf("failed to insert email: %v", err)
	}
}

func (q *postgresQueue) Dequeue(ctx context.Context) optional.Optional[Email] {
	tx, ok := ctx.Value(postgresTxKey).(*sql.Tx)
	if !ok {
		log.Fatal("cannot dequeue outside of a transaction")
	}

	var err error

	var email Email

	row := tx.QueryRow("SELECT id, recipient, subject, body FROM email_outbox LIMIT 1 FOR UPDATE SKIP LOCKED")
	if row.Err() != nil {
		log.Fatalf("failed to retrieve row: %v", err)
	}

	err = row.Scan(&email.Id, &email.Recipient, &email.Subject, &email.Body)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return optional.Empty[Email]()
		} else {
			log.Fatalf("failed to scan row: %v", err)
		}
	}

	return optional.Of(&email)
}

func (q *postgresQueue) Delete(ctx context.Context, email Email) {
	tx, ok := ctx.Value(postgresTxKey).(*sql.Tx)
	if !ok {
		log.Fatal("cannot delete outside of a transaction")
	}

	_, err := tx.Exec("DELETE FROM email_outbox WHERE id = $1", email.Id)
	if err != nil {
		log.Fatalf("failed to delete email %d: %v", email.Id, err)
	}
}
