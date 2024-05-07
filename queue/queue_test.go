package queue

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"golang.org/x/time/rate"
)

func TestOracleQueue(t *testing.T) {
	s := new(BaseTestSuite)
	s.name = "oracle"
	s.constructor = NewOracleQueue
	suite.Run(t, s)
}

func TestPostgresQueue(t *testing.T) {
	s := new(BaseTestSuite)
	s.name = "postgresql"
	s.constructor = NewPostgresQueue
	suite.Run(t, s)
}

type BaseTestSuite struct {
	suite.Suite

	name        string
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

const maxRpm = 10_000
const limit rate.Limit = rate.Limit(maxRpm) / 60
const workers = 100
const workTime = 100

func (s *BaseTestSuite) TestBenchmark() {
	l := rate.NewLimiter(limit, 1)

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx := context.Background()

		i := 0

		for i < 1000 {
			err := l.Wait(ctx)
			s.Nil(err)

			s.q.Enqueue(ctx, Email{
				Recipient: fmt.Sprintf("test_%d", i),
				Subject:   "hello",
				Body:      "world",
			})

			i++
		}
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				ctx := s.q.Begin(context.Background())
				start := time.Now()
				maybeEmail := s.q.Dequeue(ctx)
				fmt.Println(time.Since(start).Milliseconds())
				time.Sleep(time.Duration(workTime+rand.Intn(workTime/2)) * time.Millisecond)
				maybeEmail.IfPresent(func(email *Email) {
					s.q.Delete(ctx, *email)
				})
				s.q.Commit(ctx)
				if maybeEmail.IsEmpty() {
					return
				}
			}
		}()
	}

	wg.Wait()
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
