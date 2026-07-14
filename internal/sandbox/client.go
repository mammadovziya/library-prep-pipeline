package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"
)

type Client struct {
	http *http.Client
}

func NewClient(socketPath string) *Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", socketPath)
		},
		DisableCompression: true,
	}
	return &Client{http: &http.Client{Transport: transport}}
}

func (c *Client) Run(ctx context.Context, request RunRequest) (RunResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return RunResponse{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://sandboxd/v1/run", bytes.NewReader(body))
	if err != nil {
		return RunResponse{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(httpRequest)
	if err != nil {
		return RunResponse{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return RunResponse{}, errors.New("sandboxd rejected the execution request")
	}
	var result RunResponse
	if err = json.NewDecoder(response.Body).Decode(&result); err != nil {
		return RunResponse{}, err
	}
	return result, nil
}
