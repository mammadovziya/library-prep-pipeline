package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ayvazov-i/library-prep-pipeline/internal/platform"
	"github.com/google/uuid"
)

type APIClient struct {
	baseURL  string
	workerID uuid.UUID
	http     *http.Client
}

func NewAPIClient(baseURL string, workerID uuid.UUID, client *http.Client) *APIClient {
	return &APIClient{baseURL: strings.TrimRight(baseURL, "/"), workerID: workerID, http: client}
}

func (c *APIClient) Claim(ctx context.Context, taskID uuid.UUID) (platform.AttemptClaim, error) {
	var claim platform.AttemptClaim
	err := c.do(ctx, http.MethodPost, fmt.Sprintf("/internal/tasks/%s/claim", taskID), nil, &claim)
	return claim, err
}

func (c *APIClient) Heartbeat(ctx context.Context, claim platform.AttemptClaim) error {
	request := map[string]any{"attempt_id": claim.AttemptID, "fencing_token": claim.FencingToken}
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/internal/tasks/%s/heartbeat", claim.TaskID), request, nil)
}

func (c *APIClient) Commit(ctx context.Context, claim platform.AttemptClaim, artifacts []platform.ArtifactCommit, metrics json.RawMessage) error {
	request := platform.CommitAttemptRequest{AttemptID: claim.AttemptID, FencingToken: claim.FencingToken, TaskVersion: claim.TaskVersion, Artifacts: artifacts, Metrics: metrics}
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/internal/tasks/%s/commit", claim.TaskID), request, nil)
}

func (c *APIClient) Fail(ctx context.Context, claim platform.AttemptClaim, terminal bool, code, detail string) (string, error) {
	request := map[string]any{"attempt_id": claim.AttemptID, "fencing_token": claim.FencingToken, "terminal": terminal, "code": code, "detail": detail}
	var response struct {
		Disposition string `json:"message_disposition"`
	}
	err := c.do(ctx, http.MethodPost, fmt.Sprintf("/internal/tasks/%s/fail", claim.TaskID), request, &response)
	return response.Disposition, err
}

func (c *APIClient) Register(ctx context.Context, request any) error {
	return c.do(ctx, http.MethodPost, "/internal/workers/register", request, nil)
}

func (c *APIClient) Defer(ctx context.Context, taskID uuid.UUID, reason string) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/internal/tasks/%s/defer", taskID), map[string]string{"reason": reason}, nil)
}

func (c *APIClient) DeferClaim(ctx context.Context, claim platform.AttemptClaim, reason string) error {
	request := map[string]any{"reason": reason, "attempt_id": claim.AttemptID, "fencing_token": claim.FencingToken}
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/internal/tasks/%s/defer", claim.TaskID), request, nil)
}

func (c *APIClient) do(ctx context.Context, method, path string, body, output any) error {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Worker-ID", c.workerID.String())
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var problem struct {
			Code   string `json:"code"`
			Detail string `json:"detail"`
		}
		_ = json.NewDecoder(response.Body).Decode(&problem)
		if problem.Code == "stale_attempt" {
			return platform.ErrLeaseLost
		}
		if problem.Code == "not_found" {
			return platform.ErrNotFound
		}
		if problem.Code == "conflict" {
			return platform.ErrConflict
		}
		if problem.Code == "capacity_unavailable" {
			return platform.ErrCapacityUnavailable
		}
		if problem.Code == "quota_exceeded" {
			return platform.ErrQuotaExceeded
		}
		if problem.Code == "active_job_limit" {
			return platform.ErrActiveJobLimit
		}
		if problem.Code == "daily_gpu_quota" {
			return platform.ErrDailyGPUQuota
		}
		return fmt.Errorf("API request failed (%d): %s", response.StatusCode, problem.Code)
	}
	if output != nil {
		if err = json.NewDecoder(response.Body).Decode(output); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	return nil
}

func NewHTTPClient(transport *http.Transport) *http.Client {
	return &http.Client{Transport: transport, Timeout: 45 * time.Second}
}
