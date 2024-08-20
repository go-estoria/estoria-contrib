package outbox

type OutboxOption func(*Outbox) error

// WithFullEventData sets whether the outbox should include the full event data
// in each outbox row, instead of just the event ID.
//
// The default is false.
func WithFullEventData(fullData bool) OutboxOption {
	return func(s *Outbox) error {
		s.includeFullData = fullData
		return nil
	}
}
