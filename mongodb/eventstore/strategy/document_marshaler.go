package strategy

import (
	"github.com/go-estoria/estoria/eventstore"
)

type DecodeDocumentFunc func(dest any) error

type DocumentMarshaler interface {
	MarshalDocument(event *eventstore.Event) (any, error)
	UnmarshalDocument(decode DecodeDocumentFunc) (*eventstore.Event, error)
}
