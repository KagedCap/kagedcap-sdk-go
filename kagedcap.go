// Package kagedcap is the official Go SDK for KagedCap — solve reCAPTCHA v3 tokens
// with an API key.
//
//	kc := kagedcap.New(os.Getenv("KAGEDCAP_API_KEY"))
//	res, err := kc.Solve(kagedcap.SolveParams{
//	    Sitekey: "6Lc...", URL: "https://...", Action: "login", Enterprise: true,
//	})
package kagedcap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// DefaultBaseURL is the production KagedCap endpoint.
const DefaultBaseURL = "https://api.kagedcap.io"

// Tasks are the supported task types.
var Tasks = []string{
	"ReCaptchaV3Task",
	"ReCaptchaV3TaskProxyLess",
	"ReCaptchaV3EnterpriseTask",
	"ReCaptchaV3EnterpriseTaskProxyLess",
	"ReCaptchaV2Task",
	"ReCaptchaV2TaskProxyLess",
}

// Error is returned for any non-2xx response or transport failure.
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("kagedcap: %s (code=%s status=%d)", e.Message, e.Code, e.Status)
}

// SolveParams describes a solve request. Set Enterprise and/or Proxy and the task
// is chosen for you, or set Task explicitly.
type SolveParams struct {
	Sitekey    string
	URL        string
	Action     string // required for v3, ignored for v2
	Task       string // overrides Version/Enterprise/Proxy derivation when set
	Version    string // "v3" (default) or "v2" (invisible); ignored if Task is set
	Enterprise bool
	Proxy      string // omit for a ProxyLess solve
	UserAgent  string
	Device     string
	Enhanced   bool
	SecretKey  string
}

// SolveResult is the response of a successful solve.
type SolveResult struct {
	Success      bool            `json:"success"`
	Token        string          `json:"token"`
	Task         string          `json:"task"`
	Score        *float64        `json:"score"`
	Verification json.RawMessage `json:"verification"`
}

// Balance is the account balance for the API key.
type Balance struct {
	AmountMicros    string `json:"amount_micros"`
	HeldMicros      string `json:"held_micros"`
	AvailableMicros string `json:"available_micros"`
	Display         string `json:"display"`
}

// Client is a KagedCap API client. Safe for concurrent use.
type Client struct {
	apiKey  string
	baseURL string
	http    *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithBaseURL overrides the API base URL.
func WithBaseURL(u string) Option { return func(c *Client) { c.baseURL = strings.TrimRight(u, "/") } }

// WithHTTPClient supplies a custom *http.Client (e.g. a different timeout).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// New creates a client. Panics if apiKey is empty.
func New(apiKey string, opts ...Option) *Client {
	if apiKey == "" {
		panic("kagedcap: apiKey is required")
	}
	c := &Client{apiKey: apiKey, baseURL: DefaultBaseURL, http: &http.Client{Timeout: 120 * time.Second}}
	for _, o := range opts {
		o(c)
	}
	return c
}

// DeriveTask picks the task string from version, the enterprise flag, and whether a
// proxy is set. version "v2" selects reCAPTCHA v2 invisible (no enterprise variant yet).
func DeriveTask(enterprise, hasProxy bool, version string) string {
	suffix := "TaskProxyLess"
	if hasProxy {
		suffix = "Task"
	}
	if version == "v2" {
		return "ReCaptchaV2" + suffix
	}
	base := "ReCaptchaV3"
	if enterprise {
		base = "ReCaptchaV3Enterprise"
	}
	return base + suffix
}

// Solve solves a captcha and returns the token.
func (c *Client) Solve(p SolveParams) (*SolveResult, error) {
	return c.SolveContext(context.Background(), p)
}

// SolveContext is Solve with a caller-supplied context.
func (c *Client) SolveContext(ctx context.Context, p SolveParams) (*SolveResult, error) {
	task := p.Task
	if task == "" {
		task = DeriveTask(p.Enterprise, p.Proxy != "", p.Version)
	}
	body := map[string]any{"task": task, "url": p.URL, "sitekey": p.Sitekey}
	putIf(body, "action", p.Action) // omit when empty — v2 has no action, and "" fails validation
	putIf(body, "proxy", p.Proxy)
	putIf(body, "userAgent", p.UserAgent)
	putIf(body, "device", p.Device)
	putIf(body, "secretKey", p.SecretKey)
	if p.Enhanced {
		body["enhanced"] = true
	}
	var out SolveResult
	if err := c.request(ctx, http.MethodPost, "/solve", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CheckBalance returns the account balance for the API key.
func (c *Client) CheckBalance() (*Balance, error) {
	var out Balance
	if err := c.request(context.Background(), http.MethodGet, "/v1/balance", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) request(ctx context.Context, method, path string, body map[string]any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return &Error{Code: "encode_error", Message: err.Error()}
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return &Error{Code: "request_error", Message: err.Error()}
	}
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("content-type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return &Error{Code: "network_error", Message: err.Error()}
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var e struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(data, &e)
		if e.Error == "" {
			e.Error = "error"
		}
		if e.Message == "" {
			e.Message = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
		return &Error{Status: resp.StatusCode, Code: e.Error, Message: e.Message}
	}
	return json.Unmarshal(data, out)
}

func putIf(m map[string]any, k, v string) {
	if v != "" {
		m[k] = v
	}
}
