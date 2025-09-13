package jetpackio

import (
	etypeid "github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.jetify.com/typeid"
)

type typeID struct {
	tid typeid.AnyID
}

func (t typeID) String() string {
	return t.tid.String()
}

func (t typeID) TypeName() string {
	return t.tid.Prefix()
}

func (t typeID) Value() string {
	return t.tid.Suffix()
}

func (t typeID) UUID() uuid.UUID {
	return uuid.UUID(t.tid.UUIDBytes())
}

type TypeIDParser struct{}

func (p TypeIDParser) New(typ string) (etypeid.ID, error) {
	tid, err := typeid.From(typ, "")
	if err != nil {
		return etypeid.ID{}, err
	}

	return etypeid.New(typ, uuid.UUID(tid.UUIDBytes())), nil
}

func (p TypeIDParser) ParseString(s string) (etypeid.ID, error) {
	tid, err := typeid.FromString(s)
	if err != nil {
		return etypeid.ID{}, err
	}

	return etypeid.New(tid.Prefix(), uuid.UUID(tid.UUIDBytes())), nil
}
