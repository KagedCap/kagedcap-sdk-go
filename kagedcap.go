// Package kagedcap is the official Go SDK for KagedCap — solve reCAPTCHA, Ticketmaster
// tmpt, Kasada, and Ticketmaster evaluate with an API key.
//
//	kc := kagedcap.New(os.Getenv("KAGEDCAP_API_KEY"))
//	res, err := kc.Solve(kagedcap.SolveParams{
//	    Sitekey: "6Lc...", URL: "https://...", Action: "login", Enterprise: true,
//	})
//
//	// Kasada — the login result carries its headers into the reload for you.
//	login, _ := kc.KasadaLogin(kagedcap.KasadaParams{Site: "ticketmaster", Proxy: proxy})
//	fresh, _ := kc.KasadaReload(login)
//
//	// Evaluate — an EPSF allow token for the next APS step.
//	ev, _ := kc.Evaluate(kagedcap.EvaluateParams{URL: "https://auth.ticketmaster.com/...", Proxy: proxy})
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

// DefaultUserAgent is sent on reCAPTCHA and Ticketmaster solves when SolveParams.UserAgent
// is empty. It mirrors the Chrome desktop profile the solver fleet already runs, so the
// default agrees with the identity the solve is performed under instead of fighting it.
// Bump the Chrome version here and every solve follows.
const DefaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"

// Tasks are the supported task types.
var Tasks = []string{
	"ReCaptchaV3Task",
	"ReCaptchaV3TaskProxyLess",
	"ReCaptchaV3EnterpriseTask",
	"ReCaptchaV3EnterpriseTaskProxyLess",
	"ReCaptchaV2Task",
	"ReCaptchaV2TaskProxyLess",
	"TicketmasterTmptTask",
	"KasadaLogin",
	"KasadaReload",
	"EvaluateTask",
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
	UserAgent  string // defaults to DefaultUserAgent; an explicit value always wins
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

// KasadaParams describes the inputs to start a Kasada session (KasadaLogin).
type KasadaParams struct {
	Proxy string // required — the Kasada token is IP-bound, so reuse it on the target request
	Site  string // e.g. "ticketmaster"; defaults server-side when empty
	URL   string // optional informational page URL
}

// KasadaResult is the response of a Kasada solve. There is no token — replay Headers
// (user-agent + client hints) and the XKpsdk* values on your request. Pass the whole
// result to KasadaReload to refresh the session later.
type KasadaResult struct {
	Success   bool              `json:"success"`
	Task      string            `json:"task"`
	Site      string            `json:"site"`
	Headers   map[string]string `json:"headers"`
	XKpsdkCt  string            `json:"x_kpsdk_ct"`
	XKpsdkCd  string            `json:"x_kpsdk_cd"`
	XKpsdkV   string            `json:"x_kpsdk_v"`
	XKpsdkH   string            `json:"x_kpsdk_h"`
	KpsdkST   *int64            `json:"kpsdk_st"`
	// Hash is the session PoW hash (sessionHash) — resent to KasadaReload to refresh the cd.
	Hash string `json:"hash"`
	// Reload is Kasada's trust verdict: true = high-trust token.
	Reload    bool   `json:"reload"`
	UserAgent string `json:"user_agent"`
}

// EvaluateParams describes a Ticketmaster evaluate request — the EPSF step that gates
// phone verification and queue entry.
type EvaluateParams struct {
	URL   string // required — a Ticketmaster host; it also selects the default action
	Proxy string // required — "http://user:pass@host:port" (or host:port:user:pass)
	// Action overrides the action: "verify_phone" or "join_queue". Leave it empty unless the
	// caller asked for one — the solver derives it from URL (auth.* hosts verify a phone,
	// everything else joins a queue), and a default here would silently override that.
	Action      string
	PhoneNumber string // verify_phone only; include the country prefix, e.g. "+12025550123"
	QueueID     string // join_queue only; sent as queueId
	EventID     string // join_queue only; sent as eventId
	UserAgent   string // defaults to DefaultUserAgent; an explicit value always wins
}

// EvaluateResult is the response of a successful evaluate. Token is the EPSF allow token to
// replay on the next APS step; Decision is "allow", "challenge", or "block".
type EvaluateResult struct {
	Success  bool   `json:"success"`
	Task     string `json:"task"`
	Token    string `json:"token"`
	Decision string `json:"decision"`
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
	putIf(body, "userAgent", userAgentFor(task, p.UserAgent))
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

// KasadaLogin starts a Kasada session and returns the full header set. Keep the result and
// pass it to KasadaReload to refresh the session later. Proxy is required.
func (c *Client) KasadaLogin(p KasadaParams) (*KasadaResult, error) {
	return c.KasadaLoginContext(context.Background(), p)
}

// KasadaLoginContext is KasadaLogin with a caller-supplied context.
func (c *Client) KasadaLoginContext(ctx context.Context, p KasadaParams) (*KasadaResult, error) {
	body := map[string]any{"task": "KasadaLogin"}
	putIf(body, "site", p.Site)
	putIf(body, "url", p.URL)
	putIf(body, "proxy", p.Proxy)
	var out KasadaResult
	if err := c.request(ctx, http.MethodPost, "/solve", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// KasadaReload refreshes a session from a prior KasadaLogin result — its KpsdkST and
// XKpsdk* values are resent for you. No proxy needed.
func (c *Client) KasadaReload(prev *KasadaResult) (*KasadaResult, error) {
	return c.KasadaReloadContext(context.Background(), prev)
}

// KasadaReloadContext is KasadaReload with a caller-supplied context.
func (c *Client) KasadaReloadContext(ctx context.Context, prev *KasadaResult) (*KasadaResult, error) {
	if prev == nil || prev.KpsdkST == nil {
		return nil, &Error{Code: "validation_error", Message: "KasadaReload: a prior KasadaLogin result with kpsdk_st is required"}
	}
	body := map[string]any{"task": "KasadaReload", "kpsdk_st": *prev.KpsdkST}
	putIf(body, "hash", prev.Hash)
	putIf(body, "site", prev.Site)
	putIf(body, "x_kpsdk_ct", prev.XKpsdkCt)
	putIf(body, "x_kpsdk_v", prev.XKpsdkV)
	putIf(body, "x_kpsdk_h", prev.XKpsdkH)
	var out KasadaResult
	if err := c.request(ctx, http.MethodPost, "/solve", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Evaluate runs the Ticketmaster evaluate step and returns the EPSF allow token. URL and
// Proxy are required.
func (c *Client) Evaluate(p EvaluateParams) (*EvaluateResult, error) {
	return c.EvaluateContext(context.Background(), p)
}

// EvaluateContext is Evaluate with a caller-supplied context.
func (c *Client) EvaluateContext(ctx context.Context, p EvaluateParams) (*EvaluateResult, error) {
	body := map[string]any{"task": "EvaluateTask", "url": p.URL, "proxy": p.Proxy}
	putIf(body, "action", p.Action) // omit when empty so the host-based default applies
	putIf(body, "phone_number", p.PhoneNumber)
	putIf(body, "queueId", p.QueueID) // camelCase on the wire — snake_case is dropped
	putIf(body, "eventId", p.EventID)
	putIf(body, "userAgent", userAgentFor("EvaluateTask", p.UserAgent))
	var out EvaluateResult
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

// userAgentFor returns the UA to send for a task: the caller's value if they set one, else
// DefaultUserAgent. Kasada is the exception — the gateway strips userAgent for that fleet and
// the harvester reports the identity it actually used, so a default there would be a fiction.
func userAgentFor(task, userAgent string) string {
	if userAgent != "" || strings.HasPrefix(task, "Kasada") {
		return userAgent
	}
	return DefaultUserAgent
}

func putIf(m map[string]any, k, v string) {
	if v != "" {
		m[k] = v
	}
}
