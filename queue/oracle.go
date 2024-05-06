package queue

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/godror/godror"
	"github.com/moveaxlab/go-optional"
)

type oracleQueue struct {
	db *sql.DB
}

const createTable = `
CREATE TABLE email_outbox (
	id NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,
	recipient VARCHAR2(255) NOT NULL,
    subject VARCHAR2(255) NOT NULL,
	body VARCHAR2(4000) NOT NULL
)
`

const createFunction = `
CREATE OR REPLACE FUNCTION next_email RETURN NUMBER IS
    row_locked EXCEPTION;
    PRAGMA EXCEPTION_INIT(row_locked, -54);
    v_return NUMBER;
    CURSOR c_id IS
        SELECT id FROM email_outbox ORDER BY id;
BEGIN
    FOR r_id IN c_id LOOP
        BEGIN
            SELECT id INTO v_return FROM email_outbox WHERE id = r_id.id FOR UPDATE NOWAIT;
            EXIT;
        EXCEPTION WHEN row_locked THEN
            NULL;
        END;
    END LOOP;
    RETURN v_return;
END;
`

func NewOracleQueue() Queue {
	db, err := sql.Open("godror", `user="system" password="password" connectString="oracle:1521/free"`)
	if err != nil {
		log.Fatal(err)
	}
	return &oracleQueue{db: db}
}

const oracleTxKey txKey = "oracle_tx"

func (q *oracleQueue) Begin(ctx context.Context) context.Context {
	tx, err := q.db.Begin()
	if err != nil {
		log.Fatalf("failed to start transaction: %v", err)
	}
	return context.WithValue(ctx, oracleTxKey, tx)
}

func (q *oracleQueue) Commit(ctx context.Context) {
	tx, ok := ctx.Value(oracleTxKey).(*sql.Tx)
	if !ok {
		log.Fatal("cannot commit outside of a transaction")
	}
	err := tx.Commit()
	if err != nil {
		log.Fatalf("failed to commit transaction: %v", err)
	}
}

func (q *oracleQueue) Migrate() {
	var err error

	row := q.db.QueryRow("SELECT COUNT(*) FROM user_tables WHERE table_name = 'EMAIL_OUTBOX'")
	if row.Err() != nil {
		log.Fatalf("failed to check if email outbox table exists: %v", row.Err())
	}

	var count int
	err = row.Scan(&count)
	if err != nil {
		log.Fatalf("failed to scan table check: %v", err)
	}

	if count == 0 {
		_, err = q.db.Exec(createTable)
		if err != nil {
			log.Fatalf("failed to create outbox table: %v", err)
		}
	}

	_, err = q.db.Exec(createFunction)
	if err != nil {
		log.Fatalf("failed to create dequeue function: %v", err)
	}
}

func (q *oracleQueue) Truncate() {
	_, err := q.db.Exec("TRUNCATE TABLE email_outbox")
	if err != nil {
		log.Fatalf("failed to truncate outbox table: %v", err)
	}
}

func (q *oracleQueue) Enqueue(ctx context.Context, email Email) {
	_, err := q.db.Exec("INSERT INTO email_outbox (recipient, subject, body) VALUES (:1, :2, :3)", email.Recipient, email.Subject, email.Body)
	if err != nil {
		log.Fatalf("failed to insert email: %v", err)
	}
}

func (q *oracleQueue) Dequeue(ctx context.Context) optional.Optional[Email] {
	tx, ok := ctx.Value(oracleTxKey).(*sql.Tx)
	if !ok {
		log.Fatal("cannot dequeue outside of a transaction")
	}

	var id sql.NullInt64
	_, err := tx.Exec("BEGIN :1 := NEXT_EMAIL(); END;", sql.Out{Dest: &id})
	if err != nil {
		log.Fatalf("failed to get next email id: %v", err)
	}

	ok = id.Valid
	if !ok {
		return optional.Empty[Email]()
	}

	var email Email

	row := tx.QueryRow("SELECT recipient, subject, body FROM email_outbox WHERE id = :1", id.Int64)
	if row.Err() != nil {
		log.Fatalf("failed to retrieve row: %v", err)
	}

	err = row.Scan(&email.Recipient, &email.Subject, &email.Body)
	if err != nil {
		log.Fatalf("failed to scan row: %v", err)
	}

	return optional.Of(&email)
}
