package queue

import (
	"context"

	"github.com/moveaxlab/go-optional"
)

type txKey string

type Email struct {
	Id        int
	Recipient string
	Subject   string
	Body      string
}

type Queue interface {
	Migrate()
	Truncate()
	Begin(context.Context) context.Context
	Commit(context.Context)
	Count() int
	Enqueue(context.Context, Email)
	Dequeue(context.Context) optional.Optional[Email]
	Delete(context.Context, Email)
}
