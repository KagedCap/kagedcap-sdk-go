// Package kagedcap is the official Go SDK for KagedCap — solve reCAPTCHA, Ticketmaster
// tmpt, Kasada, and Ticketmaster evaluate with an API key.
//
//	kc := kagedcap.New(os.Getenv("KAGEDCAP_API_KEY"))
//	res, err := kc.Solve(kagedcap.SolveParams{
//	    Sitekey: "6Lc...", URL: "https://...", Action: "login", Enterprise: true,
//	    Deadline: 90 * time.Second,
//	})
//
// reCAPTCHA, Ticketmaster tmpt and evaluate run over the async endpoint: the SDK submits the job
// to /v2/solve and polls /v2/solve/{id} every 5s for it, blocking until the result is ready — set
// Deadline (120s by default) to bound that wait — without holding a request open for the whole
// solve. The gateway's job row carries each fleet's full result, so a poll returns exactly what
// the synchronous endpoint did.
//
// Kasada is the exception: KasadaLogin and KasadaReload POST to the synchronous /solve endpoint,
// which returns the full header set on the one request and holds the connection open for the solve.
//
//	// Kasada — the login result carries its headers into the reload for you.
//	login, _ := kc.KasadaLogin(kagedcap.KasadaParams{Site: "ticketmaster", Proxy: proxy})
//	fresh, _ := kc.KasadaReload(login)
//
//	// Evaluate — an EPSF allow token for the next APS step.
//	ev, _ := kc.Evaluate(kagedcap.EvaluateParams{URL: "https://auth.ticketmaster.com/...", Proxy: proxy})
//
// The *Deprecated methods (SolveDeprecated, KasadaLoginDeprecated, KasadaReloadDeprecated,
// EvaluateDeprecated) call the legacy synchronous /solve endpoint. SolveDeprecated and
// EvaluateDeprecated are kept for a gradual migration off the async path and will be removed;
// prefer Solve and Evaluate. KasadaLoginDeprecated and KasadaReloadDeprecated now behave exactly
// like KasadaLogin and KasadaReload, which are themselves synchronous. For manual control of the
// async families (your own poll loop, or a callback-driven flow) use the SubmitSolve + GetSolve
// primitives.
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

// DefaultSolveDeadline is the budget a solve gets for the submit plus every poll when no
// per-call Deadline is set.
const DefaultSolveDeadline = 120 * time.Second

// DefaultPollInterval is the wait between GET /v2/solve/{id} polls when no PollInterval is set.
// Measured solve_ms p50 is 1.2-2.1s, so the first poll is nearly always early — shorten this one
// constant (or set PollInterval) when we decide to chase that.
const DefaultPollInterval = 5 * time.Second

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
	// RequestID is the request_id the API puts in every error envelope. Empty for errors the
	// SDK raises on its own (timeouts, cancellation); quote it when reporting a failure.
	RequestID string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("kagedcap: %s (code=%s status=%d)", e.Message, e.Code, e.Status)
	if e.RequestID != "" {
		s += " request_id=" + e.RequestID
	}
	return s
}

// SolveParams describes a solve request. Set Enterprise and/or Proxy and the task
// is chosen for you, or set Task explicitly.
type SolveParams struct {
	Sitekey    string
	URL        string
	Action     string // optional; empty performs a no-action solve. Ignored for reCAPTCHA v2
	Task       string // overrides Version/Enterprise/Proxy derivation when set
	Version    string // "v3" (default) or "v2" (invisible); ignored if Task is set
	Enterprise bool
	Proxy      string // omit for a ProxyLess solve
	UserAgent  string // defaults to DefaultUserAgent; an explicit value always wins
	Device     string
	Enhanced   bool
	SecretKey  string
	// CallbackURL is sent as callback_url so the gateway posts the finished solve to you as
	// well. It must be https and publicly resolvable or the submit fails with
	// callback_url_invalid. The method polls either way; omitted when empty. The *Deprecated
	// methods ignore it.
	CallbackURL string
	// IdempotencyKey is sent as the Idempotency-Key header on the submit. The gateway dedupes
	// on it durably and across shards, so a caller that retries a submit whose answer it never
	// saw gets the original job back instead of paying for a second solve. Opaque, and yours
	// to generate — the SDK never invents one, since a key it made up would not survive the
	// process that would need it. The *Deprecated methods ignore it.
	IdempotencyKey string
	// Deadline is the whole budget a solve has — the submit and every poll after it. Zero means
	// DefaultSolveDeadline. A context deadline still wins if it comes first.
	Deadline time.Duration
	// PollInterval is the wait between status polls. Zero means DefaultPollInterval.
	PollInterval time.Duration
}

// SolveResult is the response of a successful reCAPTCHA or tmpt solve.
type SolveResult struct {
	Success bool   `json:"success"`
	Token   string `json:"token"`
	Task    string `json:"task"`
	// Score and Verification are reCAPTCHA-only and may be null — the gateway sends them when it
	// has them. Both are nil for tmpt. nil means "not reported", never zero; don't branch on them.
	Score        *float64        `json:"score"`
	Verification json.RawMessage `json:"verification"`
	// SolveMS is how long the solver itself took, ElapsedMS the wall clock from submit to
	// completion. Both are pointers because the gateway may send them as null or leave them
	// out entirely — nil means "not reported", never zero.
	SolveMS   *float64 `json:"solve_ms"`
	ElapsedMS *float64 `json:"elapsed_ms"`
}

// Submission is the POST /v2/solve acknowledgement (HTTP 202) — the solve is queued, not
// finished, so the only field worth anything here is the id to poll.
type Submission struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// Job is one GET /v2/solve/{id} poll. The envelope (id, status, error, timings) is always
// present; the rest is the solved result, spread at top level by the gateway and so varying by
// task family — reCAPTCHA/tmpt fill Token (+ Score/Verification), evaluate adds Decision, and
// Kasada fills Site/Headers/XKpsdk*/Hash/KpsdkST/UserAgent with no token at all. Every result
// field is cleared ~5 minutes after completion, so a late poll reads a done job with them empty;
// the typed methods detect that and return result_expired rather than an empty result.
//
// `success` is deliberately not decoded: it is true only once status is "done", which makes it a
// redundant restatement of status rather than the completion test.
type Job struct {
	ID        string   `json:"id"`
	Status    string   `json:"status"` // "running" | "done" | "failed"
	Error     string   `json:"error"`
	SolveMS   *float64 `json:"solve_ms"`
	ElapsedMS *float64 `json:"elapsed_ms"`

	// Result fields (present on "done" until the sweep clears them).
	Task         string            `json:"task"`
	Token        string            `json:"token"`
	Score        *float64          `json:"score"`
	Verification json.RawMessage   `json:"verification"`
	Decision     string            `json:"decision"`
	Site         string            `json:"site"`
	Headers      map[string]string `json:"headers"`
	XKpsdkCt     string            `json:"x_kpsdk_ct"`
	XKpsdkCd     string            `json:"x_kpsdk_cd"`
	XKpsdkV      string            `json:"x_kpsdk_v"`
	XKpsdkH      string            `json:"x_kpsdk_h"`
	KpsdkST      *int64            `json:"kpsdk_st"`
	Hash         string            `json:"hash"`
	Reload       bool              `json:"reload"`
	UserAgent    string            `json:"user_agent"`
}

// KasadaParams describes the inputs to start a Kasada session (KasadaLogin).
type KasadaParams struct {
	Proxy string // required — the Kasada token is IP-bound, so reuse it on the target request
	Site  string // e.g. "ticketmaster"; defaults server-side when empty
	URL   string // optional informational page URL
	// Deadline and PollInterval are accepted for source compatibility but no longer apply:
	// KasadaLogin and KasadaReload are synchronous, so there is no poll loop to bound. The
	// client's own http.Client timeout is the only bound, as with the other synchronous methods.
	Deadline     time.Duration
	PollInterval time.Duration
}

// KasadaResult is the response of a Kasada solve. There is no token — replay Headers
// (user-agent + client hints) and the XKpsdk* values on your request. Pass the whole
// result to KasadaReload to refresh the session later.
type KasadaResult struct {
	Success  bool              `json:"success"`
	Task     string            `json:"task"`
	Site     string            `json:"site"`
	Headers  map[string]string `json:"headers"`
	XKpsdkCt string            `json:"x_kpsdk_ct"`
	XKpsdkCd string            `json:"x_kpsdk_cd"`
	XKpsdkV  string            `json:"x_kpsdk_v"`
	XKpsdkH  string            `json:"x_kpsdk_h"`
	KpsdkST  *int64            `json:"kpsdk_st"`
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
	// Deadline / PollInterval bound the async wait; zero means the defaults. Evaluate drives a
	// full BotGuard VM, so leave Deadline at the 120s default unless you know your flow is faster.
	Deadline     time.Duration
	PollInterval time.Duration
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
	// Subscriptions lists the solve packages on the key that made the call; empty when it has none.
	Subscriptions []Subscription `json:"subscriptions"`
}

// Subscription is a solve package on the calling API key: a fixed number of solves of one
// type per period, drawn on instead of balance.
type Subscription struct {
	ID          string  `json:"id"`
	Package     string  `json:"package"`
	SKU         string  `json:"sku"`
	Period      string  `json:"period"` // "week" | "month"
	Status      string  `json:"status"` // "active" | "canceling" | "past_due"
	Quota       int64   `json:"quota"`
	Used        int64   `json:"used"`
	Remaining   int64   `json:"remaining"`
	PeriodStart *string `json:"period_start"`
	PeriodEnd   *string `json:"period_end"`
	// RenewsAt is when the next period starts and Used resets; nil when canceling or past due.
	RenewsAt *string `json:"renews_at"`
	// ExpiresAt is when the package ends after a cancel; nil otherwise.
	ExpiresAt *string `json:"expires_at"`
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

// Solve solves a reCAPTCHA or Ticketmaster tmpt captcha and returns the token. It submits to
// /v2/solve and polls every PollInterval until the solver is done, so it blocks for the length
// of the solve just as it always has — without holding a request open for it. Give up after
// Deadline. For Kasada use KasadaLogin/KasadaReload (no token result); for the evaluate
// decision use Evaluate.
func (c *Client) Solve(p SolveParams) (*SolveResult, error) {
	return c.SolveContext(context.Background(), p)
}

// SolveContext is Solve with a caller-supplied context. Cancelling ctx stops the poll loop,
// and whichever runs out first — ctx's deadline or SolveParams.Deadline — ends the wait.
func (c *Client) SolveContext(ctx context.Context, p SolveParams) (*SolveResult, error) {
	task := taskFor(p)
	if isKasadaTask(task) {
		// A Kasada result has no token, so SolveResult cannot represent it — steer the caller to
		// the method that returns the header set rather than hand back an empty, "successful" token.
		return nil, &Error{Code: "validation_error", Message: "Solve does not return a Kasada result; use KasadaLogin or KasadaReload"}
	}
	ctx, cancel := withDeadline(ctx, p.Deadline)
	defer cancel()

	sub, err := c.SubmitSolveContext(ctx, p)
	if err != nil {
		return nil, solveWaitErr(ctx, "submitting the solve", err)
	}
	job, err := c.poll(ctx, sub.ID, pollInterval(p.PollInterval), "waiting for solve "+sub.ID)
	if err != nil {
		return nil, err
	}
	if job.Token == "" {
		// The gateway clears results roughly five minutes after completion (and a reCAPTCHA token
		// is dead inside two anyway), so "done" with no token is a late read of a finished job,
		// not a solve that produced an empty string — returning "" would look like success.
		return nil, expiredErr("solve", sub.ID)
	}
	return job.toSolveResult(task), nil
}

// SubmitSolve submits a reCAPTCHA or tmpt solve to /v2/solve and returns the job id to poll with
// GetSolve, without waiting. Use it to drive your own poll loop or a callback-driven flow; most
// callers want Solve, which submits and polls for you.
func (c *Client) SubmitSolve(p SolveParams) (*Submission, error) {
	return c.SubmitSolveContext(context.Background(), p)
}

// SubmitSolveContext is SubmitSolve with a caller-supplied context.
func (c *Client) SubmitSolveContext(ctx context.Context, p SolveParams) (*Submission, error) {
	task := taskFor(p)
	if isKasadaTask(task) {
		return nil, &Error{Code: "validation_error", Message: "SubmitSolve does not handle Kasada; use KasadaLogin or KasadaReload"}
	}
	body := solveBody(p, task)
	putIf(body, "callback_url", p.CallbackURL)
	return c.submitRaw(ctx, body, p.IdempotencyKey)
}

// GetSolve fetches one job by id (GET /v2/solve/{id}). Status is "running", "done" or "failed";
// on "done" the result fields are populated until the ~5-minute sweep clears them. It is the
// poll primitive behind every async method, exposed for manual or callback-driven use.
func (c *Client) GetSolve(id string) (*Job, error) {
	return c.GetSolveContext(context.Background(), id)
}

// GetSolveContext is GetSolve with a caller-supplied context.
func (c *Client) GetSolveContext(ctx context.Context, id string) (*Job, error) {
	if id == "" {
		return nil, &Error{Code: "validation_error", Message: "GetSolve: id is required"}
	}
	var job Job
	if err := c.request(ctx, http.MethodGet, "/v2/solve/"+id, nil, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// SolveDeprecated solves a captcha over the legacy synchronous /solve endpoint, which holds
// the HTTP connection open for the whole solve. Behaviour is exactly what Solve did before
// v2; the async fields (CallbackURL, IdempotencyKey, Deadline, PollInterval) do not apply and
// are ignored — the client's own http.Client timeout is the only bound.
//
// Deprecated: use Solve, which submits to /v2/solve and polls for the result.
func (c *Client) SolveDeprecated(p SolveParams) (*SolveResult, error) {
	return c.SolveDeprecatedContext(context.Background(), p)
}

// SolveDeprecatedContext is SolveDeprecated with a caller-supplied context.
//
// Deprecated: use SolveContext.
func (c *Client) SolveDeprecatedContext(ctx context.Context, p SolveParams) (*SolveResult, error) {
	var out SolveResult
	if err := c.request(ctx, http.MethodPost, "/solve", solveBody(p, taskFor(p)), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// KasadaLogin starts a Kasada session and returns the full header set. Keep the result and
// pass it to KasadaReload to refresh the session later. Proxy is required. POSTs to the
// synchronous /solve endpoint and returns the result directly — no submit-and-poll.
func (c *Client) KasadaLogin(p KasadaParams) (*KasadaResult, error) {
	return c.KasadaLoginContext(context.Background(), p)
}

// KasadaLoginContext is KasadaLogin with a caller-supplied context.
func (c *Client) KasadaLoginContext(ctx context.Context, p KasadaParams) (*KasadaResult, error) {
	return c.kasadaSync(ctx, kasadaLoginBody(p))
}

// KasadaReload refreshes a session from a prior KasadaLogin result — its KpsdkST and
// XKpsdk* values are resent for you. No proxy needed. POSTs to the synchronous /solve endpoint
// and returns the result directly — no submit-and-poll.
func (c *Client) KasadaReload(prev *KasadaResult) (*KasadaResult, error) {
	return c.KasadaReloadContext(context.Background(), prev)
}

// KasadaReloadContext is KasadaReload with a caller-supplied context.
func (c *Client) KasadaReloadContext(ctx context.Context, prev *KasadaResult) (*KasadaResult, error) {
	body, err := kasadaReloadBody(prev)
	if err != nil {
		return nil, err
	}
	return c.kasadaSync(ctx, body)
}

// KasadaLoginDeprecated starts a Kasada session over the synchronous /solve endpoint.
//
// Deprecated: use KasadaLogin, which is now synchronous and behaves identically.
func (c *Client) KasadaLoginDeprecated(p KasadaParams) (*KasadaResult, error) {
	return c.kasadaSync(context.Background(), kasadaLoginBody(p))
}

// KasadaReloadDeprecated refreshes a session over the synchronous /solve endpoint.
//
// Deprecated: use KasadaReload, which is now synchronous and behaves identically.
func (c *Client) KasadaReloadDeprecated(prev *KasadaResult) (*KasadaResult, error) {
	body, err := kasadaReloadBody(prev)
	if err != nil {
		return nil, err
	}
	return c.kasadaSync(context.Background(), body)
}

// Evaluate runs the Ticketmaster evaluate step and returns the EPSF allow token. URL and
// Proxy are required. Submits to /v2/solve and polls for the result.
func (c *Client) Evaluate(p EvaluateParams) (*EvaluateResult, error) {
	return c.EvaluateContext(context.Background(), p)
}

// EvaluateContext is Evaluate with a caller-supplied context.
func (c *Client) EvaluateContext(ctx context.Context, p EvaluateParams) (*EvaluateResult, error) {
	ctx, cancel := withDeadline(ctx, p.Deadline)
	defer cancel()
	sub, err := c.submitRaw(ctx, evaluateBody(p), "")
	if err != nil {
		return nil, solveWaitErr(ctx, "submitting the evaluate", err)
	}
	job, err := c.poll(ctx, sub.ID, pollInterval(p.PollInterval), "waiting for evaluate "+sub.ID)
	if err != nil {
		return nil, err
	}
	// Decision, not token: a challenge verdict legitimately carries an empty token, so an empty
	// decision is the only reliable "result cleared" signal for evaluate.
	if job.Decision == "" {
		return nil, expiredErr("evaluate", sub.ID)
	}
	return job.toEvaluateResult(), nil
}

// EvaluateDeprecated runs evaluate over the legacy synchronous /solve endpoint.
//
// Deprecated: use Evaluate, which submits to /v2/solve and polls for the result.
func (c *Client) EvaluateDeprecated(p EvaluateParams) (*EvaluateResult, error) {
	var out EvaluateResult
	if err := c.request(context.Background(), http.MethodPost, "/solve", evaluateBody(p), &out); err != nil {
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

// kasadaSync POSTs a Kasada body to the synchronous /solve endpoint and decodes the full result
// directly — no submit-and-poll. It backs KasadaLogin/KasadaReload and their *Deprecated aliases.
func (c *Client) kasadaSync(ctx context.Context, body map[string]any) (*KasadaResult, error) {
	var out KasadaResult
	if err := c.request(ctx, http.MethodPost, "/solve", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// submitRaw POSTs a prepared body to /v2/solve and returns the acknowledgement.
func (c *Client) submitRaw(ctx context.Context, body map[string]any, idempotencyKey string) (*Submission, error) {
	var sub Submission
	if err := c.request(ctx, http.MethodPost, "/v2/solve", body, &sub, withIdempotencyKey(idempotencyKey)); err != nil {
		return nil, err
	}
	if sub.ID == "" {
		// The submit answered 202 without an id, so there is nothing to poll and no way to find
		// the job again. Gateway bug, not the caller's — name it as one.
		return nil, &Error{Code: "internal_error", Message: "/v2/solve accepted the job but returned no id"}
	}
	return &sub, nil
}

// poll loops GET /v2/solve/{id} every interval until the job is done (returns the Job), failed
// (returns the gateway's error code) or ctx runs out. An unfamiliar status means keep waiting:
// treating one as terminal would break the SDK the day the gateway adds one.
func (c *Client) poll(ctx context.Context, id string, interval time.Duration, where string) (*Job, error) {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, solveWaitErr(ctx, where, ctx.Err())
		case <-timer.C:
		}
		job, err := c.GetSolveContext(ctx, id)
		if err != nil {
			// Includes not_found (404), which the gateway also returns for a job belonging to
			// another account. Either way the id will never resolve, so stop rather than burn
			// the deadline on it.
			return nil, solveWaitErr(ctx, where, err)
		}
		switch job.Status {
		case "done":
			return job, nil
		case "failed":
			// The poll body carries error alone — no separate human message — so it lands in
			// Code, where v1 callers already switch on the gateway's own codes.
			code, detail := job.Error, job.Error
			if code == "" {
				code, detail = "solve_failed", "no reason given"
			}
			return nil, &Error{Code: code, Message: "solve " + id + " failed: " + detail}
		}
		timer.Reset(interval)
	}
}

func (j *Job) toSolveResult(task string) *SolveResult {
	t := j.Task
	if t == "" {
		// Older gateways did not echo the task in the poll body; fall back to the one submitted.
		t = task
	}
	return &SolveResult{Success: true, Token: j.Token, Task: t, Score: j.Score, Verification: j.Verification, SolveMS: j.SolveMS, ElapsedMS: j.ElapsedMS}
}

func (j *Job) toEvaluateResult() *EvaluateResult {
	return &EvaluateResult{Success: true, Task: j.Task, Token: j.Token, Decision: j.Decision}
}

// request performs one API call. decorate runs after the standard headers are set, which is
// how the v2 submit adds its Idempotency-Key without every other call site growing a header
// argument it would always pass empty. Any 2xx counts as success — the v2 submit answers 202,
// everything else 200.
func (c *Client) request(ctx context.Context, method, path string, body map[string]any, out any, decorate ...func(*http.Request)) error {
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
	for _, d := range decorate {
		d(req)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return &Error{Code: "network_error", Message: err.Error()}
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var e struct {
			Error     string `json:"error"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		}
		_ = json.Unmarshal(data, &e)
		if e.Error == "" {
			e.Error = "error"
		}
		if e.Message == "" {
			e.Message = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
		return &Error{Status: resp.StatusCode, Code: e.Error, Message: e.Message, RequestID: e.RequestID}
	}
	return json.Unmarshal(data, out)
}

// withIdempotencyKey sets the Idempotency-Key header when the caller supplied one. Empty is
// the normal case and sends no header at all, which is what the gateway expects.
func withIdempotencyKey(key string) func(*http.Request) {
	return func(r *http.Request) {
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
	}
}

// isKasadaTask reports whether a task is a Kasada one — the family whose result has no token and
// so cannot be returned as a SolveResult.
func isKasadaTask(task string) bool { return strings.HasPrefix(task, "Kasada") }

func taskFor(p SolveParams) string {
	if p.Task != "" {
		return p.Task
	}
	return DeriveTask(p.Enterprise, p.Proxy != "", p.Version)
}

// solveBody builds the solve request body shared by /v2/solve and the legacy /solve — they
// take the same JSON, and v2 only adds an optional callback_url on top.
func solveBody(p SolveParams, task string) map[string]any {
	body := map[string]any{"task": task, "url": p.URL, "sitekey": p.Sitekey}
	putIf(body, "action", p.Action) // omit when empty — reCAPTCHA v2 has no action, and "" fails validation
	putIf(body, "proxy", p.Proxy)
	putIf(body, "userAgent", userAgentFor(task, p.UserAgent))
	putIf(body, "device", p.Device)
	putIf(body, "secretKey", p.SecretKey)
	if p.Enhanced {
		body["enhanced"] = true
	}
	return body
}

func kasadaLoginBody(p KasadaParams) map[string]any {
	body := map[string]any{"task": "KasadaLogin"}
	putIf(body, "site", p.Site)
	putIf(body, "url", p.URL)
	putIf(body, "proxy", p.Proxy)
	return body
}

func kasadaReloadBody(prev *KasadaResult) (map[string]any, error) {
	if prev == nil || prev.KpsdkST == nil {
		return nil, &Error{Code: "validation_error", Message: "KasadaReload: a prior KasadaLogin result with kpsdk_st is required"}
	}
	body := map[string]any{"task": "KasadaReload", "kpsdk_st": *prev.KpsdkST}
	putIf(body, "hash", prev.Hash)
	putIf(body, "site", prev.Site)
	putIf(body, "x_kpsdk_ct", prev.XKpsdkCt)
	putIf(body, "x_kpsdk_v", prev.XKpsdkV)
	putIf(body, "x_kpsdk_h", prev.XKpsdkH)
	return body, nil
}

func evaluateBody(p EvaluateParams) map[string]any {
	body := map[string]any{"task": "EvaluateTask", "url": p.URL, "proxy": p.Proxy}
	putIf(body, "action", p.Action) // omit when empty so the host-based default applies
	putIf(body, "phone_number", p.PhoneNumber)
	putIf(body, "queueId", p.QueueID) // camelCase on the wire — snake_case is dropped
	putIf(body, "eventId", p.EventID)
	putIf(body, "userAgent", userAgentFor("EvaluateTask", p.UserAgent))
	return body
}

// withDeadline bounds a solve's whole wait (submit + polls). Zero uses DefaultSolveDeadline; a
// caller's own ctx deadline still wins if it is sooner.
func withDeadline(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		d = DefaultSolveDeadline
	}
	return context.WithTimeout(ctx, d)
}

func pollInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultPollInterval
	}
	return d
}

func expiredErr(kind, id string) *Error {
	return &Error{Code: "result_expired", Message: kind + " " + id + " completed but its result has already been cleared; results are kept for about five minutes"}
}

// solveWaitErr keeps the poll loop speaking the SDK's error type: a context that ran out is a
// solve_timeout whichever deadline fired, the caller's own or the per-call Deadline. Anything
// else is a real transport or API failure and travels untouched.
func solveWaitErr(ctx context.Context, where string, err error) error {
	switch ctx.Err() {
	case context.DeadlineExceeded:
		return &Error{Code: "solve_timeout", Message: "deadline exceeded " + where}
	case context.Canceled:
		return &Error{Code: "canceled", Message: "context canceled " + where}
	}
	return err
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
