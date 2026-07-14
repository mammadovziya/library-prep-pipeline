package platform

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultGlobalStorageCeiling int64 = 800_000_000_000
	DefaultLeaseDuration              = 90 * time.Second
	DefaultHeartbeatInterval          = 20 * time.Second
)

type Job struct {
	ID                  uuid.UUID  `json:"id"`
	ParentJobID         *uuid.UUID `json:"parent_job_id,omitempty"`
	OwnerSub            string     `json:"-"`
	Status              string     `json:"status"`
	Preset              string     `json:"preset"`
	RequestedConformers int        `json:"requested_conformers"`
	AlgorithmVersion    string     `json:"algorithm_version"`
	OptimisticVersion   int64      `json:"version"`
	CleanupPending      bool       `json:"cleanup_pending,omitempty"`
	FailureCode         *string    `json:"failure_code,omitempty"`
	FailureDetail       *string    `json:"failure_detail,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	ExpiresAt           *time.Time `json:"expires_at,omitempty"`
}

type ReservationEstimate struct {
	RetainedInputBytes       int64 `json:"retained_input_bytes"`
	PredictedWorkingSetBytes int64 `json:"predicted_working_set_bytes"`
	PredictedFinalBytes      int64 `json:"predicted_final_output_bytes"`
	FinalizationMarginBytes  int64 `json:"finalization_margin_bytes"`
	RetryMarginBytes         int64 `json:"retry_attempt_margin_bytes"`
	MultipartMarginBytes     int64 `json:"incomplete_multipart_margin_bytes"`
}

func (r ReservationEstimate) PeakBytes() int64 {
	return r.RetainedInputBytes + r.PredictedWorkingSetBytes + r.PredictedFinalBytes +
		r.FinalizationMarginBytes + r.RetryMarginBytes + r.MultipartMarginBytes
}

type CreateJobRequest struct {
	ParentJobID         *uuid.UUID          `json:"parent_job_id,omitempty"`
	InputUploadID       uuid.UUID           `json:"input_upload_id"`
	Preset              string              `json:"preset"`
	RequestedConformers int                 `json:"requested_conformers"`
	AlgorithmVersion    string              `json:"algorithm_version"`
	Reservation         ReservationEstimate `json:"reservation"`
}

type ArtifactCommit struct {
	Kind           string          `json:"kind"`
	Bucket         string          `json:"bucket"`
	ObjectKey      string          `json:"object_key"`
	SizeBytes      int64           `json:"size_bytes"`
	ChecksumSHA256 string          `json:"checksum_sha256"`
	MediaType      string          `json:"media_type"`
	Manifest       json.RawMessage `json:"manifest"`
}

type AttemptClaim struct {
	TaskID                 uuid.UUID       `json:"task_id"`
	JobID                  uuid.UUID       `json:"job_id"`
	AttemptID              uuid.UUID       `json:"attempt_id"`
	AttemptNumber          int             `json:"attempt_number"`
	ExecutionAttemptNumber int             `json:"execution_attempt_number"`
	FencingToken           int64           `json:"fencing_token"`
	TaskVersion            int64           `json:"task_version"`
	LeaseExpiresAt         time.Time       `json:"lease_expires_at"`
	RequiredGPUType        string          `json:"required_gpu_type"`
	TaskSpec               json.RawMessage `json:"task_spec"`
}

type CommitAttemptRequest struct {
	AttemptID    uuid.UUID        `json:"attempt_id"`
	FencingToken int64            `json:"fencing_token"`
	TaskVersion  int64            `json:"task_version"`
	Artifacts    []ArtifactCommit `json:"artifacts"`
	Metrics      json.RawMessage  `json:"metrics"`
}

type ProgressEvent struct {
	JobID     uuid.UUID       `json:"job_id"`
	Sequence  int64           `json:"sequence"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

type ProgressSnapshot struct {
	JobID              uuid.UUID       `json:"job_id"`
	Sequence           int64           `json:"sequence"`
	Stage              string          `json:"stage"`
	CompletedUnits     int64           `json:"completed_units"`
	TotalUnits         *int64          `json:"total_units,omitempty"`
	ApproximatePercent *float64        `json:"approximate_percent,omitempty"`
	Detail             json.RawMessage `json:"detail"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

type OutboxEvent struct {
	ID              uuid.UUID       `json:"id"`
	AggregateType   string          `json:"aggregate_type"`
	AggregateID     uuid.UUID       `json:"aggregate_id"`
	EventType       string          `json:"event_type"`
	Subject         string          `json:"subject"`
	Payload         json.RawMessage `json:"payload"`
	PublishAttempts int             `json:"publish_attempts"`
}

type Upload struct {
	ID               uuid.UUID `json:"id"`
	Bucket           string    `json:"bucket"`
	ObjectKey        string    `json:"object_key"`
	Status           string    `json:"status"`
	ExpectedBytes    int64     `json:"expected_bytes"`
	ExpectedChecksum string    `json:"expected_checksum"`
	PartSizeBytes    int64     `json:"part_size_bytes"`
	MaxParts         int       `json:"max_parts"`
	ExpiresAt        time.Time `json:"expires_at"`
	ProviderUploadID *string   `json:"provider_upload_id,omitempty"`
}

type Artifact struct {
	ID             uuid.UUID       `json:"id"`
	JobID          uuid.UUID       `json:"job_id"`
	Kind           string          `json:"kind"`
	SizeBytes      int64           `json:"size_bytes"`
	ChecksumSHA256 string          `json:"checksum_sha256"`
	MediaType      string          `json:"media_type"`
	Manifest       json.RawMessage `json:"manifest"`
	CreatedAt      time.Time       `json:"created_at"`
	ExpiresAt      *time.Time      `json:"expires_at,omitempty"`
}

type CreateUploadRequest struct {
	ExpectedBytes    int64  `json:"expected_bytes"`
	ChecksumSHA256   string `json:"checksum_sha256"`
	OriginalFilename string `json:"original_filename"`
}

type MultipartCompletionPart struct {
	PartNumber     int32
	ETag           string
	ChecksumBase64 string
}

type Identity struct {
	Subject   string
	Roles     map[string]bool
	ExpiresAt time.Time
}

func (i Identity) HasRole(role string) bool { return i.Roles[role] }
