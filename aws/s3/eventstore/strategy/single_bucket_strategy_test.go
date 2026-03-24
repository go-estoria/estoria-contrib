package strategy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

// mockS3 implements the S3 interface with configurable function fields
type mockS3 struct {
	ListObjectsV2Fn func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	PutObjectFn     func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObjectFn     func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

func (m *mockS3) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.ListObjectsV2Fn != nil {
		return m.ListObjectsV2Fn(ctx, params, optFns...)
	}
	return &s3.ListObjectsV2Output{}, nil
}

func (m *mockS3) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.PutObjectFn != nil {
		return m.PutObjectFn(ctx, params, optFns...)
	}
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.GetObjectFn != nil {
		return m.GetObjectFn(ctx, params, optFns...)
	}
	return &s3.GetObjectOutput{}, nil
}

func TestNewSingleBucketStrategy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		client    *s3.Client
		bucket    string
		wantError bool
		errorMsg  string
	}{
		{
			name:      "nil client returns error",
			client:    nil,
			bucket:    "test-bucket",
			wantError: true,
			errorMsg:  "client is required",
		},
		{
			name:      "empty bucket returns error",
			client:    &s3.Client{},
			bucket:    "",
			wantError: true,
			errorMsg:  "bucket is required",
		},
		{
			name:      "valid args succeed",
			client:    &s3.Client{},
			bucket:    "test-bucket",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			strategy, err := NewSingleBucketStrategy(tt.client, tt.bucket)

			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errorMsg)
				}
				if err.Error() != tt.errorMsg {
					t.Errorf("expected error %q, got %q", tt.errorMsg, err.Error())
				}
				if strategy != nil {
					t.Errorf("expected nil strategy, got %+v", strategy)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if strategy == nil {
					t.Fatal("expected non-nil strategy")
				}
				if strategy.bucket != tt.bucket {
					t.Errorf("expected bucket %q, got %q", tt.bucket, strategy.bucket)
				}
			}
		})
	}
}

func TestDefaultBucketKeyResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		aggregateID typeid.ID
		version     int64
		expected    string
	}{
		{
			name: "formats key with type/uuid/version.json",
			aggregateID: typeid.ID{
				Type: "user",
				UUID: uuid.Must(uuid.FromString("550e8400-e29b-41d4-a716-446655440000")),
			},
			version:  1,
			expected: "user/550e8400-e29b-41d4-a716-446655440000/1.json",
		},
		{
			name: "handles version 0",
			aggregateID: typeid.ID{
				Type: "order",
				UUID: uuid.Must(uuid.FromString("12345678-1234-1234-1234-123456789012")),
			},
			version:  0,
			expected: "order/12345678-1234-1234-1234-123456789012/0.json",
		},
		{
			name: "handles large version number",
			aggregateID: typeid.ID{
				Type: "product",
				UUID: uuid.Must(uuid.FromString("abcdef00-1234-5678-9012-abcdefabcdef")),
			},
			version:  999999,
			expected: "product/abcdef00-1234-5678-9012-abcdefabcdef/999999.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := DefaultBucketKeyResolver(tt.aggregateID, tt.version)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestGetLatestVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		mockObjects     []types.Object
		mockError       error
		expectedVersion int64
		wantError       bool
	}{
		{
			name:            "empty results return version 0",
			mockObjects:     []types.Object{},
			expectedVersion: 0,
			wantError:       false,
		},
		{
			name: "single object returns correct version",
			mockObjects: []types.Object{
				{Key: aws.String("user/abc-123/5.json")},
			},
			expectedVersion: 5,
			wantError:       false,
		},
		{
			name: "returns highest version from multiple objects",
			mockObjects: []types.Object{
				{Key: aws.String("user/abc-123/1.json")},
				{Key: aws.String("user/abc-123/10.json")},
				{Key: aws.String("user/abc-123/2.json")},
				{Key: aws.String("user/abc-123/5.json")},
			},
			expectedVersion: 10,
			wantError:       false,
		},
		{
			name: "regression: correctly parses version from full S3 key path (Task 15 fix)",
			mockObjects: []types.Object{
				{Key: aws.String("user/550e8400-e29b-41d4-a716-446655440000/1.json")},
				{Key: aws.String("user/550e8400-e29b-41d4-a716-446655440000/2.json")},
				{Key: aws.String("user/550e8400-e29b-41d4-a716-446655440000/10.json")},
			},
			expectedVersion: 10,
			wantError:       false,
		},
		{
			name:            "NotFound error returns version 0",
			mockError:       &types.NotFound{},
			expectedVersion: 0,
			wantError:       false,
		},
		{
			name:            "NoSuchKey error returns version 0",
			mockError:       &types.NoSuchKey{},
			expectedVersion: 0,
			wantError:       false,
		},
		{
			name:            "NoSuchBucket error returns version 0",
			mockError:       &types.NoSuchBucket{},
			expectedVersion: 0,
			wantError:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mock := &mockS3{
				ListObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
					if tt.mockError != nil {
						return nil, tt.mockError
					}
					return &s3.ListObjectsV2Output{
						Contents: tt.mockObjects,
					}, nil
				},
			}

			strategy := &SingleBucketStrategy{
				s3:         mock,
				bucket:     "test-bucket",
				resolveKey: DefaultBucketKeyResolver,
				marshaler:  JSONObjectMarshaler{},
				log:        estoria.GetLogger(),
			}

			streamID := typeid.ID{
				Type: "user",
				UUID: uuid.Must(uuid.FromString("550e8400-e29b-41d4-a716-446655440000")),
			}

			version, err := strategy.getLatestVersion(context.Background(), streamID)

			if tt.wantError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if version != tt.expectedVersion {
					t.Errorf("expected version %d, got %d", tt.expectedVersion, version)
				}
			}
		})
	}
}

func TestInsertStreamEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		existingVersion   int64
		writableEvents    []*eventstore.WritableEvent
		expectVersion     int64
		wantError         bool
		errorMsg          string
		verifyEventIDType string
	}{
		{
			name:            "creates events with correct IDs and versions",
			existingVersion: 0,
			writableEvents: []*eventstore.WritableEvent{
				{Type: "UserCreated", Data: []byte(`{"name": "Alice"}`)},
				{Type: "UserUpdated", Data: []byte(`{"email": "alice@example.com"}`)},
			},
			expectVersion:     0,
			verifyEventIDType: "UserCreated",
		},
		{
			name:            "appends to existing stream",
			existingVersion: 5,
			writableEvents: []*eventstore.WritableEvent{
				{Type: "OrderPlaced", Data: []byte(`{"amount": 100}`)},
			},
			expectVersion:     5,
			verifyEventIDType: "OrderPlaced",
		},
		{
			name:            "version mismatch returns error",
			existingVersion: 3,
			writableEvents: []*eventstore.WritableEvent{
				{Type: "TestEvent", Data: []byte(`{}`)},
			},
			expectVersion: 5,
			wantError:     true,
			errorMsg:      "expected version 5, but stream has version 3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var putObjectCalls []string

			mock := &mockS3{
				ListObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
					if tt.existingVersion == 0 {
						return &s3.ListObjectsV2Output{Contents: []types.Object{}}, nil
					}
					objects := make([]types.Object, tt.existingVersion)
					for i := int64(1); i <= tt.existingVersion; i++ {
						key := aws.String(fmt.Sprintf("user/abc-123/%d.json", i))
						objects[i-1] = types.Object{Key: key}
					}
					return &s3.ListObjectsV2Output{Contents: objects}, nil
				},
				PutObjectFn: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					putObjectCalls = append(putObjectCalls, *params.Key)
					return &s3.PutObjectOutput{}, nil
				},
			}

			strategy := &SingleBucketStrategy{
				s3:         mock,
				bucket:     "test-bucket",
				resolveKey: DefaultBucketKeyResolver,
				marshaler:  JSONObjectMarshaler{},
				log:        estoria.GetLogger(),
			}

			streamID := typeid.ID{
				Type: "user",
				UUID: uuid.Must(uuid.FromString("00000000-0000-0000-0000-000000000abc")),
			}

			opts := eventstore.AppendStreamOptions{}
			if tt.expectVersion > 0 {
				opts.ExpectVersion = eventstore.VersionPtr(tt.expectVersion)
			}

			before := time.Now()
			result, err := strategy.InsertStreamEvents(context.Background(), streamID, tt.writableEvents, opts)
			after := time.Now()

			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errorMsg)
				}
				if err.Error() != tt.errorMsg {
					t.Errorf("expected error %q, got %q", tt.errorMsg, err.Error())
				}
				if result != nil {
					t.Errorf("expected nil result, got %+v", result)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result == nil {
				t.Fatal("expected non-nil result")
			}

			if len(result.InsertedEvents) != len(tt.writableEvents) {
				t.Fatalf("expected %d inserted events, got %d", len(tt.writableEvents), len(result.InsertedEvents))
			}

			// Verify event properties
			for i, event := range result.InsertedEvents {
				// Verify ID type matches event type
				if tt.verifyEventIDType != "" && event.ID.Type != tt.writableEvents[i].Type {
					t.Errorf("event %d: expected ID type %q, got %q", i, tt.writableEvents[i].Type, event.ID.Type)
				}

				// Verify ID UUID is valid (non-zero)
				if event.ID.UUID == (uuid.UUID{}) {
					t.Errorf("event %d: expected non-nil UUID", i)
				}

				// Verify stream ID
				if event.StreamID != streamID {
					t.Errorf("event %d: expected stream ID %v, got %v", i, streamID, event.StreamID)
				}

				// Verify version sequence
				expectedVersion := tt.existingVersion + int64(i) + 1
				if event.StreamVersion != expectedVersion {
					t.Errorf("event %d: expected version %d, got %d", i, expectedVersion, event.StreamVersion)
				}

				// Verify timestamp is reasonable
				if event.Timestamp.Before(before) || event.Timestamp.After(after) {
					t.Errorf("event %d: timestamp %v is outside expected range [%v, %v]", i, event.Timestamp, before, after)
				}

				// Verify data (compare as strings since they're byte slices)
				if string(event.Data) != string(tt.writableEvents[i].Data) {
					t.Errorf("event %d: expected data %s, got %s", i, tt.writableEvents[i].Data, event.Data)
				}
			}

			// Verify PutObject was called correctly
			if len(putObjectCalls) != len(tt.writableEvents) {
				t.Errorf("expected %d PutObject calls, got %d", len(tt.writableEvents), len(putObjectCalls))
			}
		})
	}
}

func TestGetStreamIterator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		streamID           typeid.ID
		readOpts           eventstore.ReadStreamOptions
		expectedFromVer    int64
		expectedCurrentVer int64
		expectedToVer      int64
	}{
		{
			name: "creates iterator with default options",
			streamID: typeid.ID{
				Type: "user",
				UUID: uuid.Must(uuid.FromString("00000000-0000-0000-0000-000000000001")),
			},
			readOpts:           eventstore.ReadStreamOptions{},
			expectedFromVer:    0,
			expectedCurrentVer: 0,
			expectedToVer:      0,
		},
		{
			name: "creates iterator with offset",
			streamID: typeid.ID{
				Type: "order",
				UUID: uuid.Must(uuid.FromString("00000000-0000-0000-0000-000000000002")),
			},
			readOpts: eventstore.ReadStreamOptions{
				AfterVersion: 5,
			},
			expectedFromVer:    5,
			expectedCurrentVer: 5,
			expectedToVer:      0,
		},
		{
			name: "creates iterator with offset and count",
			streamID: typeid.ID{
				Type: "product",
				UUID: uuid.Must(uuid.FromString("00000000-0000-0000-0000-000000000003")),
			},
			readOpts: eventstore.ReadStreamOptions{
				AfterVersion: 10,
				Count:        20,
			},
			expectedFromVer:    10,
			expectedCurrentVer: 10,
			expectedToVer:      30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var capturedListInput *s3.ListObjectsV2Input

			mock := &mockS3{
				ListObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
					capturedListInput = params
					return &s3.ListObjectsV2Output{}, nil
				},
				GetObjectFn: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader([]byte("{}"))),
					}, nil
				},
			}

			strategy := &SingleBucketStrategy{
				s3:         mock,
				bucket:     "test-bucket",
				resolveKey: DefaultBucketKeyResolver,
				marshaler:  JSONObjectMarshaler{},
				log:        estoria.GetLogger(),
			}

			iter, err := strategy.GetStreamIterator(context.Background(), tt.streamID, tt.readOpts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if iter == nil {
				t.Fatal("expected non-nil iterator")
			}

			// Verify iterator is of correct type
			streamIter, ok := iter.(*streamIterator)
			if !ok {
				t.Fatalf("expected *streamIterator, got %T", iter)
			}

			// Verify iterator fields
			if streamIter.streamID != tt.streamID {
				t.Errorf("expected stream ID %v, got %v", tt.streamID, streamIter.streamID)
			}

			if streamIter.bucket != "test-bucket" {
				t.Errorf("expected bucket %q, got %q", "test-bucket", streamIter.bucket)
			}

			if streamIter.fromVersion != tt.expectedFromVer {
				t.Errorf("expected fromVersion %d, got %d", tt.expectedFromVer, streamIter.fromVersion)
			}

			if streamIter.currentVersion != tt.expectedCurrentVer {
				t.Errorf("expected currentVersion %d, got %d", tt.expectedCurrentVer, streamIter.currentVersion)
			}

			if streamIter.toVersion != tt.expectedToVer {
				t.Errorf("expected toVersion %d, got %d", tt.expectedToVer, streamIter.toVersion)
			}

			// Note: We can't directly verify paginator setup without triggering a fetch,
			// but we can verify the ListObjectsV2 was NOT called during GetStreamIterator
			// (it should only be called during iteration)
			if capturedListInput != nil {
				t.Error("ListObjectsV2 should not be called during GetStreamIterator creation")
			}
		})
	}
}
