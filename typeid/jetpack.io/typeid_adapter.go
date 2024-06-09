package jetpackio

import (
	etypeid "github.com/go-estoria/estoria/typeid"
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

type TypeIDParser struct{}

func (p TypeIDParser) New(typ string) (etypeid.TypeID, error) {
	tid, err := typeid.From(typ, "")
	if err != nil {
		return nil, err
	}

	return typeID{tid: tid}, nil
}

func (p TypeIDParser) ParseString(s string) (etypeid.TypeID, error) {
	tid, err := typeid.FromString(s)
	if err != nil {
		return nil, err
	}

	return typeID{tid: tid}, nil
}
