package api

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ayvazov-i/library-prep-pipeline/internal/objectstore"
	"github.com/ayvazov-i/library-prep-pipeline/internal/platform"
	"github.com/google/uuid"
)

const maxJSONBody = 1 << 20

type Server struct {
	store               *platform.Store
	objects             *objectstore.Client
	auth                *Authenticator
	lease               time.Duration
	log                 *slog.Logger
	ready               func(context.Context) error
	internalDevelopment bool
}

func NewServer(store *platform.Store, objects *objectstore.Client, auth *Authenticator, lease time.Duration, log *slog.Logger, ready func(context.Context) error, internalDevelopment bool) *Server {
	return &Server{store: store, objects: objects, auth: auth, lease: lease, log: log, ready: ready, internalDevelopment: internalDevelopment}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("GET /v1/jobs", s.auth.Middleware(http.HandlerFunc(s.handleListJobs)))
	mux.Handle("POST /v1/jobs", s.auth.Middleware(http.HandlerFunc(s.handleCreateJob)))
	mux.Handle("GET /v1/jobs/{jobID}", s.auth.Middleware(http.HandlerFunc(s.handleGetJob)))
	mux.Handle("POST /v1/jobs/{jobID}/cancel", s.auth.Middleware(http.HandlerFunc(s.handleCancelJob)))
	mux.Handle("POST /v1/jobs/{jobID}/rerun", s.auth.Middleware(http.HandlerFunc(s.handleRerunJob)))
	mux.Handle("GET /v1/jobs/{jobID}/events", s.auth.Middleware(http.HandlerFunc(s.handleEvents)))
	mux.Handle("GET /v1/jobs/{jobID}/artifacts", s.auth.Middleware(http.HandlerFunc(s.handleListArtifacts)))
	mux.Handle("POST /v1/artifacts/{artifactID}/download", s.auth.Middleware(http.HandlerFunc(s.handleDownloadArtifact)))
	mux.Handle("POST /v1/uploads", s.auth.Middleware(http.HandlerFunc(s.handleCreateUpload)))
	mux.Handle("POST /v1/uploads/{uploadID}/parts", s.auth.Middleware(http.HandlerFunc(s.handlePresignPart)))
	mux.Handle("POST /v1/uploads/{uploadID}/complete", s.auth.Middleware(http.HandlerFunc(s.handleCompleteUpload)))
	mux.Handle("POST /internal/tasks/{taskID}/claim", s.internalOnly(http.HandlerFunc(s.handleClaimTask)))
	mux.Handle("POST /internal/tasks/{taskID}/heartbeat", s.internalOnly(http.HandlerFunc(s.handleHeartbeat)))
	mux.Handle("POST /internal/tasks/{taskID}/commit", s.internalOnly(http.HandlerFunc(s.handleCommit)))
	mux.Handle("POST /internal/tasks/{taskID}/fail", s.internalOnly(http.HandlerFunc(s.handleFail)))
	mux.Handle("POST /internal/tasks/{taskID}/defer", s.internalOnly(http.HandlerFunc(s.handleDefer)))
	mux.Handle("POST /internal/workers/register", s.internalOnly(http.HandlerFunc(s.handleRegisterWorker)))
	return requestMiddleware(mux, s.log)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.ready(ctx); err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "not_ready", "required dependency is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	identity := identityFrom(r.Context())
	jobs, err := s.store.ListJobs(r.Context(), identity.Subject, 50)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	var req platform.CreateJobRequest
	if err = json.Unmarshal(body, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_json", "request body is not valid JSON")
		return
	}
	if req.Preset == "" || req.AlgorithmVersion == "" || req.RequestedConformers < 1 || req.RequestedConformers > 10 {
		writeProblem(w, http.StatusUnprocessableEntity, "invalid_job", "preset, algorithm_version and 1-10 conformers are required")
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) < 8 || len(idempotencyKey) > 128 {
		writeProblem(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key must contain 8-128 characters")
		return
	}
	identity := identityFrom(r.Context())
	job, replay, err := s.store.CreateJob(r.Context(), identity.Subject, idempotencyKey, platform.RequestHash(body), req)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if replay {
		w.Header().Set("Idempotent-Replay", "true")
	}
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("jobID"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_job_id", "job ID is invalid")
		return
	}
	job, err := s.store.GetJob(r.Context(), identityFrom(r.Context()).Subject, id)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("jobID"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_job_id", "job ID is invalid")
		return
	}
	version, err := strconv.ParseInt(r.Header.Get("If-Match"), 10, 64)
	if err != nil {
		writeProblem(w, http.StatusPreconditionRequired, "version_required", "If-Match must contain the current numeric job version")
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) < 8 || len(idempotencyKey) > 128 {
		writeProblem(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key must contain 8-128 characters")
		return
	}
	requestHash := platform.RequestHash([]byte(id.String() + "|" + r.Header.Get("If-Match")))
	job, replay, err := s.store.CancelJob(r.Context(), identityFrom(r.Context()).Subject, id, version, idempotencyKey, requestHash)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if replay {
		w.Header().Set("Idempotent-Replay", "true")
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleRerunJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("jobID"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_job_id", "job ID is invalid")
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) < 8 || len(idempotencyKey) > 128 {
		writeProblem(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key must contain 8-128 characters")
		return
	}
	job, replay, err := s.store.RerunJob(r.Context(), identityFrom(r.Context()).Subject, id, idempotencyKey, platform.RequestHash([]byte(id.String())))
	if err != nil {
		s.writeError(w, err)
		return
	}
	if replay {
		w.Header().Set("Idempotent-Replay", "true")
	}
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	identity := identityFrom(r.Context())
	jobID, err := uuid.Parse(r.PathValue("jobID"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_job_id", "job ID is invalid")
		return
	}
	if _, err = s.store.GetJob(r.Context(), identity.Subject, jobID); err != nil {
		s.writeError(w, err)
		return
	}
	lastEventID := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	after, _ := strconv.ParseInt(lastEventID, 10, 64)
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "stream_unavailable", "streaming is unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	write := func(value string) bool {
		if err := controller.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			return false
		}
		if _, err := io.WriteString(w, value); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	if lastEventID == "" {
		snapshot, snapshotErr := s.store.ProgressSnapshot(r.Context(), identity.Subject, jobID)
		if snapshotErr != nil {
			return
		}
		payload, marshalErr := json.Marshal(snapshot)
		if marshalErr != nil || !write(fmt.Sprintf("id: %d\nevent: snapshot\ndata: %s\n\n", snapshot.Sequence, payload)) {
			return
		}
		after = snapshot.Sequence
	}
	heartbeat := time.NewTicker(15 * time.Second)
	poll := time.NewTicker(time.Second)
	tokenExpiry := time.NewTimer(max(time.Until(identity.ExpiresAt), time.Millisecond))
	defer heartbeat.Stop()
	defer poll.Stop()
	defer tokenExpiry.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-tokenExpiry.C:
			return
		case <-heartbeat.C:
			if s.store.CheckAccount(r.Context(), identity.Subject) != nil {
				return
			}
			if !write(": heartbeat\n\n") {
				return
			}
		case <-poll.C:
			events, queryErr := s.store.ProgressEvents(r.Context(), identity.Subject, jobID, after, 100)
			if queryErr != nil {
				return
			}
			for _, event := range events {
				if !write(fmt.Sprintf("id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.EventType, event.Payload)) {
					return
				}
				after = event.Sequence
			}
		}
	}
}

func (s *Server) handleListArtifacts(w http.ResponseWriter, r *http.Request) {
	identity := identityFrom(r.Context())
	jobID, err := uuid.Parse(r.PathValue("jobID"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_job_id", "job ID must be a UUID")
		return
	}
	artifacts, err := s.store.ListArtifacts(r.Context(), identity.Subject, jobID)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"artifacts": artifacts})
}

func (s *Server) handleDownloadArtifact(w http.ResponseWriter, r *http.Request) {
	identity := identityFrom(r.Context())
	artifactID, err := uuid.Parse(r.PathValue("artifactID"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_artifact_id", "artifact ID must be a UUID")
		return
	}
	bucket, objectKey, err := s.store.AuthorizeArtifactDownload(r.Context(), identity.Subject, artifactID, 45*time.Minute)
	if err != nil {
		s.writeError(w, err)
		return
	}
	url, err := s.objects.PresignDownload(r.Context(), bucket, objectKey, 15*time.Minute)
	if err != nil {
		s.log.Error("presign artifact download", "artifact_id", artifactID, "error", err)
		writeProblem(w, http.StatusServiceUnavailable, "object_store_unavailable", "download URL could not be created")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"url": url, "expires_in_seconds": 900})
}

func (s *Server) handleCreateUpload(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	var req platform.CreateUploadRequest
	if err = json.Unmarshal(body, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_json", "request body is not valid JSON")
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) < 8 || len(idempotencyKey) > 128 {
		writeProblem(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key must contain 8-128 characters")
		return
	}
	identity := identityFrom(r.Context())
	upload, replay, err := s.store.CreateUpload(r.Context(), identity.Subject, idempotencyKey, platform.RequestHash(body), "library-inputs", req)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if upload.ProviderUploadID == nil {
		providerID, createErr := s.objects.CreateMultipart(r.Context(), upload.Bucket, upload.ObjectKey)
		if createErr != nil {
			s.writeError(w, createErr)
			return
		}
		if createErr = s.store.SetMultipartProviderID(r.Context(), identity.Subject, upload.ID, providerID); createErr != nil {
			_ = s.objects.AbortMultipart(r.Context(), upload.Bucket, upload.ObjectKey, providerID)
			s.writeError(w, createErr)
			return
		}
		upload.ProviderUploadID = &providerID
		upload.Status = "uploading"
	}
	if replay {
		w.Header().Set("Idempotent-Replay", "true")
	}
	writeJSON(w, http.StatusCreated, upload)
}

func (s *Server) handlePresignPart(w http.ResponseWriter, r *http.Request) {
	uploadID, err := uuid.Parse(r.PathValue("uploadID"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_upload_id", "upload ID is invalid")
		return
	}
	var req struct {
		PartNumber     int    `json:"part_number"`
		SizeBytes      int64  `json:"size_bytes"`
		ChecksumBase64 string `json:"checksum_sha256_base64"`
	}
	if err = decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	identity := identityFrom(r.Context())
	upload, err := s.store.GetUpload(r.Context(), identity.Subject, uploadID)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if upload.ProviderUploadID == nil || req.PartNumber < 1 || req.PartNumber > upload.MaxParts || req.SizeBytes < 1 || req.SizeBytes > upload.PartSizeBytes {
		writeProblem(w, http.StatusUnprocessableEntity, "invalid_part", "part number or size is outside the server-owned upload contract")
		return
	}
	decodedChecksum, checksumErr := base64.StdEncoding.DecodeString(req.ChecksumBase64)
	if checksumErr != nil || len(decodedChecksum) != sha256.Size {
		writeProblem(w, http.StatusUnprocessableEntity, "invalid_part_checksum", "part checksum must be base64-encoded SHA-256")
		return
	}
	if err = s.store.RecordSignedPart(r.Context(), identity.Subject, uploadID, req.PartNumber, req.SizeBytes, req.ChecksumBase64); err != nil {
		s.writeError(w, err)
		return
	}
	url, err := s.objects.PresignPart(r.Context(), upload.Bucket, upload.ObjectKey, *upload.ProviderUploadID, int32(req.PartNumber), req.ChecksumBase64, 15*time.Minute)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"url": url, "expires_in_seconds": 900})
}

func (s *Server) handleCompleteUpload(w http.ResponseWriter, r *http.Request) {
	uploadID, err := uuid.Parse(r.PathValue("uploadID"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_upload_id", "upload ID is invalid")
		return
	}
	var req struct {
		Parts []objectstore.CompletedPart `json:"parts"`
	}
	body, err := readBody(r)
	if err != nil || json.Unmarshal(body, &req) != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_json", "request body is not valid JSON")
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) < 8 || len(idempotencyKey) > 128 {
		writeProblem(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key must contain 8-128 characters")
		return
	}
	requestHash := platform.RequestHash(body)
	identity := identityFrom(r.Context())
	upload, err := s.store.GetUpload(r.Context(), identity.Subject, uploadID)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if upload.ProviderUploadID == nil {
		writeProblem(w, http.StatusConflict, "upload_not_started", "multipart upload has not been initialized")
		return
	}
	if upload.Status == "completed" {
		replay, completeErr := s.store.CompleteUpload(r.Context(), identity.Subject, uploadID, idempotencyKey, requestHash, upload.ExpectedBytes, upload.ExpectedChecksum)
		if completeErr != nil {
			s.writeError(w, completeErr)
			return
		}
		if replay {
			w.Header().Set("Idempotent-Replay", "true")
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "completed"})
		return
	}
	completionParts := make([]platform.MultipartCompletionPart, 0, len(req.Parts))
	for _, part := range req.Parts {
		completionParts = append(completionParts, platform.MultipartCompletionPart{PartNumber: part.PartNumber, ETag: part.ETag, ChecksumBase64: part.ChecksumSHA256})
	}
	if err = s.store.ValidateMultipartCompletion(r.Context(), identity.Subject, uploadID, completionParts); err != nil {
		s.writeError(w, err)
		return
	}
	if err = s.objects.VerifyObject(r.Context(), upload.Bucket, upload.ObjectKey, upload.ExpectedBytes, upload.ExpectedChecksum); err != nil {
		if err = s.objects.CompleteMultipart(r.Context(), upload.Bucket, upload.ObjectKey, *upload.ProviderUploadID, req.Parts); err != nil {
			s.writeError(w, err)
			return
		}
		if err = s.objects.VerifyObject(r.Context(), upload.Bucket, upload.ObjectKey, upload.ExpectedBytes, upload.ExpectedChecksum); err != nil {
			s.writeError(w, err)
			return
		}
	}
	replay, err := s.store.CompleteUpload(r.Context(), identity.Subject, uploadID, idempotencyKey, requestHash, upload.ExpectedBytes, upload.ExpectedChecksum)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if replay {
		w.Header().Set("Idempotent-Replay", "true")
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "completed"})
}

func (s *Server) handleClaimTask(w http.ResponseWriter, r *http.Request) {
	taskID, workerID, ok := parseInternalIDs(w, r)
	if !ok {
		return
	}
	claim, err := s.store.ClaimTask(r.Context(), taskID, workerID, s.lease)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, claim)
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	taskID, workerID, ok := parseInternalIDs(w, r)
	if !ok {
		return
	}
	var req struct {
		AttemptID    uuid.UUID `json:"attempt_id"`
		FencingToken int64     `json:"fencing_token"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	expires, err := s.store.HeartbeatAttempt(r.Context(), taskID, req.AttemptID, workerID, req.FencingToken, s.lease)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lease_expires_at": expires})
}

func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request) {
	taskID, workerID, ok := parseInternalIDs(w, r)
	if !ok {
		return
	}
	var req platform.CommitAttemptRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if err := s.store.CommitAttempt(r.Context(), taskID, workerID, req); err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "committed"})
}

func (s *Server) handleFail(w http.ResponseWriter, r *http.Request) {
	taskID, workerID, ok := parseInternalIDs(w, r)
	if !ok {
		return
	}
	var req struct {
		AttemptID    uuid.UUID `json:"attempt_id"`
		FencingToken int64     `json:"fencing_token"`
		Terminal     bool      `json:"terminal"`
		Code         string    `json:"code"`
		Detail       string    `json:"detail"`
	}
	if err := decodeJSON(r, &req); err != nil || req.AttemptID == uuid.Nil || req.Code == "" {
		writeProblem(w, http.StatusBadRequest, "invalid_failure", "attempt, fencing token and failure code are required")
		return
	}
	disposition, err := s.store.FailAttempt(r.Context(), taskID, req.AttemptID, workerID, req.FencingToken, req.Terminal, req.Code, req.Detail)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded", "message_disposition": disposition})
}

func (s *Server) handleDefer(w http.ResponseWriter, r *http.Request) {
	taskID, workerID, ok := parseInternalIDs(w, r)
	if !ok {
		return
	}
	var req struct {
		Reason       string     `json:"reason"`
		AttemptID    *uuid.UUID `json:"attempt_id,omitempty"`
		FencingToken int64      `json:"fencing_token,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil || req.Reason == "" {
		writeProblem(w, http.StatusBadRequest, "invalid_deferral", "a deferral reason is required")
		return
	}
	var err error
	if req.AttemptID != nil {
		err = s.store.ReleaseClaimForCapacity(r.Context(), taskID, *req.AttemptID, workerID, req.FencingToken, req.Reason)
	} else {
		err = s.store.RecordDeliveryDeferral(r.Context(), taskID, workerID, req.Reason)
	}
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deferred"})
}

func (s *Server) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	workerID, err := uuid.Parse(r.Header.Get("X-Worker-ID"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_worker_id", "worker ID must be a UUID")
		return
	}
	var req struct {
		Name             string          `json:"name"`
		GPUUUID          string          `json:"gpu_uuid"`
		GPUType          string          `json:"gpu_type"`
		ImageDigest      string          `json:"image_digest"`
		DriverVersion    string          `json:"driver_version"`
		Capabilities     []string        `json:"capabilities"`
		MaxConcurrency   int             `json:"max_concurrency"`
		FreeScratchBytes int64           `json:"free_scratch_bytes"`
		Preflight        json.RawMessage `json:"preflight"`
	}
	if err = decodeJSON(r, &req); err != nil || req.Name == "" || req.GPUUUID == "" || req.ImageDigest == "" || req.MaxConcurrency < 1 || req.MaxConcurrency > 8 {
		writeProblem(w, http.StatusBadRequest, "invalid_worker", "worker registration is incomplete")
		return
	}
	identity := "development-worker"
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		identity = r.TLS.PeerCertificates[0].Subject.CommonName
	}
	if !s.internalDevelopment && (identity != req.Name || !strings.HasPrefix(identity, "worker-")) {
		writeProblem(w, http.StatusForbidden, "worker_identity_mismatch", "certificate identity does not match worker name")
		return
	}
	if err = s.store.RegisterWorker(r.Context(), workerID, req.Name, identity, req.GPUUUID, req.GPUType, req.ImageDigest, req.DriverVersion, req.Capabilities, req.MaxConcurrency, req.FreeScratchBytes, req.Preflight); err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

func (s *Server) internalOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.internalDevelopment && r.Header.Get("X-Dev-Internal") == "true" {
			next.ServeHTTP(w, r)
			return
		}
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			writeProblem(w, http.StatusUnauthorized, "mtls_required", "a verified worker certificate is required")
			return
		}
		if r.URL.Path != "/internal/workers/register" {
			workerID, err := uuid.Parse(r.Header.Get("X-Worker-ID"))
			if err != nil || s.store.AuthorizeWorkerIdentity(r.Context(), workerID, r.TLS.PeerCertificates[0].Subject.CommonName) != nil {
				writeProblem(w, http.StatusForbidden, "worker_identity_mismatch", "worker ID is not bound to this certificate")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func parseInternalIDs(w http.ResponseWriter, r *http.Request) (uuid.UUID, uuid.UUID, bool) {
	taskID, taskErr := uuid.Parse(r.PathValue("taskID"))
	workerID, workerErr := uuid.Parse(r.Header.Get("X-Worker-ID"))
	if taskErr != nil || workerErr != nil {
		writeProblem(w, http.StatusBadRequest, "invalid_identity", "task and worker IDs must be UUIDs")
		return uuid.Nil, uuid.Nil, false
	}
	return taskID, workerID, true
}

func (s *Server) writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, platform.ErrNotFound):
		writeProblem(w, http.StatusNotFound, "not_found", "resource was not found")
	case errors.Is(err, platform.ErrQuotaExceeded):
		writeProblem(w, http.StatusTooManyRequests, "quota_exceeded", "account quota is exhausted")
	case errors.Is(err, platform.ErrActiveJobLimit):
		writeProblem(w, http.StatusTooManyRequests, "active_job_limit", "another job is already active")
	case errors.Is(err, platform.ErrDailyGPUQuota):
		writeProblem(w, http.StatusTooManyRequests, "daily_gpu_quota", "daily GPU quota is exhausted")
	case errors.Is(err, platform.ErrCapacityUnavailable):
		writeProblem(w, http.StatusServiceUnavailable, "capacity_unavailable", "peak storage reservation does not fit")
	case errors.Is(err, platform.ErrLeaseLost):
		writeProblem(w, http.StatusConflict, "stale_attempt", "attempt lease or fencing token is stale")
	case errors.Is(err, platform.ErrConflict):
		writeProblem(w, http.StatusConflict, "conflict", "request conflicts with current state")
	default:
		s.log.Error("request failed", "error", err)
		writeProblem(w, http.StatusInternalServerError, "internal_error", "request could not be completed")
	}
}

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONBody+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxJSONBody {
		return nil, errors.New("request body is too large")
	}
	return body, nil
}

func decodeJSON(r *http.Request, target any) error {
	body, err := readBody(r)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(target); err != nil {
		return err
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain exactly one JSON value")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeProblem(w http.ResponseWriter, status int, code, detail string) {
	writeJSON(w, status, map[string]any{"type": "about:blank", "status": status, "code": code, "detail": detail})
}

func requestMiddleware(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", requestID)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		start := time.Now()
		next.ServeHTTP(w, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		log.Info("request", "method", r.Method, "route", route, "request_id", requestID, "duration_ms", time.Since(start).Milliseconds())
	})
}
