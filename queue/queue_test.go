package queue

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

func TestOracleQueue(t *testing.T) {
	s := new(BaseTestSuite)
	s.constructor = NewOracleQueue
	suite.Run(t, s)
}

func TestPostgresQueue(t *testing.T) {
	s := new(BaseTestSuite)
	s.constructor = NewPostgresQueue
	suite.Run(t, s)
}

type BaseTestSuite struct {
	suite.Suite

	constructor func() Queue
	q           Queue
}

func (s *BaseTestSuite) SetupSuite() {
	s.q = s.constructor()
	s.q.Migrate()
}

func (s *BaseTestSuite) SetupTest() {
	s.q.Truncate()
}

func (s *BaseTestSuite) TestNothingToDo() {
	ctx := s.q.Begin(context.Background())
	defer s.q.Commit(ctx)

	maybeEmail := s.q.Dequeue(ctx)
	s.False(maybeEmail.IsPresent())
}

func (s *BaseTestSuite) TestBenchmark() {
	for i := 0; i < 1000; i++ {
		s.q.Enqueue(context.Background(), Email{
			Recipient: fmt.Sprintf("test_%d", i),
			Subject:   "hello",
			Body:      "world",
		})
	}

	txs := make([]context.Context, 0, 100)

	for i := 0; i < 100; i++ {
		ctx := s.q.Begin(context.Background())
		txs = append(txs, ctx)

		start := time.Now()
		maybeEmail := s.q.Dequeue(ctx)
		log.Printf("dequeue #%d done in %v", i, time.Since(start))
		s.True(maybeEmail.IsPresent())
	}

	for _, tx := range txs {
		s.q.Commit(tx)
	}
}

func (s *BaseTestSuite) TestWorkDivision() {
	s.q.Enqueue(context.Background(), Email{
		Recipient: "test1",
		Subject:   "hello",
		Body:      "world",
	})
	s.q.Enqueue(context.Background(), Email{
		Recipient: "test2",
		Subject:   "hello",
		Body:      "world",
	})

	tx1 := s.q.Begin(context.Background())
	defer s.q.Commit(tx1)

	email1 := s.q.Dequeue(tx1)
	s.True(email1.IsPresent())
	s.Equal("test1", email1.Get().Recipient)
	s.Equal("hello", email1.Get().Subject)
	s.Equal("world", email1.Get().Body)

	tx2 := s.q.Begin(context.Background())
	defer s.q.Commit(tx2)

	email2 := s.q.Dequeue(tx2)
	s.True(email2.IsPresent())
	s.Equal("test2", email2.Get().Recipient)
	s.Equal("hello", email2.Get().Subject)
	s.Equal("world", email2.Get().Body)

	tx3 := s.q.Begin(context.Background())
	defer s.q.Commit(tx3)

	email3 := s.q.Dequeue(tx3)
	s.False(email3.IsPresent())
}
