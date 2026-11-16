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
//
// Transient failures — HTTP 429, 500, 502, 503, 504 and transport errors — are retried on the v2
// submit and poll with jittered exponential backoff until the deadline; see RetryPolicy,
// WithRetryPolicy and WithoutRetries. Every v2 submit carries an Idempotency-Key, generated per
// call when SolveParams does not set one, so a retried submit can never become a second billable
// solve. The legacy /solve (SolveDeprecated, EvaluateDeprecated) carries one too, and is retried
// only on failures that never reached the gateway (transport errors, the edge's own non-JSON
// 429/502/503/504) and never more than 100s after its first attempt, since the gateway's claim on
// the key lives 120s. Kasada carries no key and is sent exactly once.
package kagedcap

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"strconv"
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

// Retry defaults. See RetryPolicy for what each one bounds.
const (
	DefaultRetryInitialBackoff = 250 * time.Millisecond
	DefaultRetryMaxBackoff     = 5 * time.Second
	DefaultRetryMaxRetryAfter  = 10 * time.Second
)

// MinRetryDelay is the floor under every wait between tries, whatever the policy or a Retry-After
// header says: a Retry-After of 0 (or one that would not parse) falls back to the backoff, and the
// backoff itself never goes below this, so no client ever hammers the edge in a tight loop.
const MinRetryDelay = 50 * time.Millisecond

// maxRetryAfter caps what a Retry-After header can claim, as exposed on Error.RetryAfter; the wait
// it produces is capped again by RetryPolicy.MaxRetryAfter. Clamped before any Duration arithmetic
// so a header naming the year 9999 or an overflowing count of seconds can never wrap negative.
const maxRetryAfter = 24 * time.Hour

// minAttemptBudget is how much of the deadline a retry needs ahead of it to be worth sending: with
// less than this left the answer could not arrive in time, so the run reports its last real
// response instead of starting an attempt the deadline would cut short.
const minAttemptBudget = 500 * time.Millisecond

// legacyRetryWindow is how long after its first attempt a legacy /solve call may still start a
// retry. The gateway claims the Idempotency-Key for 120s (SET NX EX 120); a resend after that
// claim has lapsed is not deduplicated but run — and billed — as a fresh solve, so the window
// stops 20s short of it. A variable, not a constant, only so a test can shrink it.
var legacyRetryWindow = 100 * time.Second

// Tasks are the supported task types.
var Tasks = []string{
	"ReCaptchaV3Task",
	"ReCaptchaV3TaskProxyLess",
	"ReCaptchaV3EnterpriseTask",
	"ReCaptchaV3EnterpriseTaskProxyLess",
	"ReCaptchaV2Task",
	"ReCaptchaV2TaskProxyLess",
	"TicketmasterTmptTask",
	// Same solve and exchange as TicketmasterTmptTask, but the solver PROVES the cookie with
	// a HEAD to the caller's own URL before returning it, re-solving on its own egress (and
	// walking Chrome majors) when the edge refuses. Up to four billed solves in one call, so
	// a caller that does not want that latency keeps TicketmasterTmptTask.
	"TicketmasterTmptTaskEnterprise",
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
	// Attempts is how many times the request went out before this error came back: 1 for a
	// terminal answer, more when transient failures (429, 5xx, transport) were retried until the
	// deadline or RetryPolicy.MaxAttempts ran out. 0 for errors raised before any request was made.
	Attempts int
	// RetryAfter is the Retry-After the last response carried, capped at 24h; zero when it had
	// none or it would not parse.
	RetryAfter time.Duration
	// IdempotencyKey is the key the request carried — yours, or the one the SDK generated — on the
	// v2 submit or the legacy /solve, so a failed call can be matched to the job the gateway may
	// already hold. Empty for Kasada, which carries none, and off the poll.
	IdempotencyKey string

	// envelope is set when the body was our own JSON error envelope: the answer came from the
	// gateway, not from the Railway/Cloudflare edge in front of it. Decides the legacy /solve retry.
	envelope bool
}

func (e *Error) Error() string {
	s := fmt.Sprintf("kagedcap: %s (code=%s status=%d)", e.Message, e.Code, e.Status)
	if e.RequestID != "" {
		s += " request_id=" + e.RequestID
	}
	return s
}

// describe is the one-line form used when a retry run reports its last answer.
func (e *Error) describe() string {
	if e.Status == 0 {
		return e.Code + ": " + e.Message
	}
	return fmt.Sprintf("HTTP %d %s: %s", e.Status, e.Code, e.Message)
}

// RetryPolicy bounds how the client retries transient failures — HTTP 429, 500, 502, 503, 504
// and transport errors (refused/reset connections, EOF, timeouts, DNS) — on the v2 submit and the
// v2 status poll, and the narrower set the legacy /solve allows (see SolveDeprecated). Since
// 2026-10-20 the Railway edge in front of the gateway sheds hundreds of thousands of submits an
// hour with a bare 429 before they reach the API, and the API itself answers 429/503 under load;
// those requests succeed a moment later, so failing the solve on the first one lost work for nothing.
//
// Retries wait with exponential backoff — InitialBackoff, doubling, capped at MaxBackoff — with
// full jitter (uniform in [MinRetryDelay, backoff]); a Retry-After header is honoured instead,
// capped at MaxRetryAfter. No wait is ever shorter than MinRetryDelay. They stop at the caller's
// deadline (Deadline / the context) — a retry is not started with under 500ms of it left — or at
// MaxAttempts when that is set. The zero (or negative) value of any duration means its Default*
// constant.
type RetryPolicy struct {
	// MaxAttempts caps the tries, the first one included; 0 means retry until the deadline.
	MaxAttempts int
	// InitialBackoff is the wait after the first failure; it doubles on each one after.
	InitialBackoff time.Duration
	// MaxBackoff caps the doubling.
	MaxBackoff time.Duration
	// MaxRetryAfter caps how long a Retry-After header can hold a retry back.
	MaxRetryAfter time.Duration
	// Disabled turns retries off: every transient failure is returned at once, as before.
	Disabled bool
}

// DefaultRetryPolicy is the policy New installs: retry until the deadline, 250ms doubling to 5s
// with full jitter, Retry-After honoured up to 10s.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{InitialBackoff: DefaultRetryInitialBackoff, MaxBackoff: DefaultRetryMaxBackoff, MaxRetryAfter: DefaultRetryMaxRetryAfter}
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	d := DefaultRetryPolicy()
	if p.InitialBackoff <= 0 {
		p.InitialBackoff = d.InitialBackoff
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = d.MaxBackoff
	}
	if p.MaxRetryAfter <= 0 {
		p.MaxRetryAfter = d.MaxRetryAfter
	}
	return p
}

// Delay is the wait before the next try after `failures` consecutive transient failures (1 after
// the first). retryAfter is what the last response asked for, or zero: when set it wins, capped at
// MaxRetryAfter and unjittered, since the server named the moment it wants the retry at. Either
// way the result is never under MinRetryDelay. Exposed so a caller driving SubmitSolve/GetSolve
// by hand can pace its own loop the same way.
func (p RetryPolicy) Delay(failures int, retryAfter time.Duration) time.Duration {
	p = p.withDefaults()
	if retryAfter > 0 {
		if retryAfter > p.MaxRetryAfter {
			retryAfter = p.MaxRetryAfter
		}
		if retryAfter < MinRetryDelay {
			return MinRetryDelay
		}
		return retryAfter
	}
	// Double by loop rather than shift, stopping at the cap: no overflow for any input.
	backoff := p.InitialBackoff
	for i := 1; i < failures && backoff < p.MaxBackoff; i++ {
		if backoff > p.MaxBackoff/2 {
			backoff = p.MaxBackoff
			break
		}
		backoff *= 2
	}
	if backoff > p.MaxBackoff {
		backoff = p.MaxBackoff
	}
	if backoff <= MinRetryDelay {
		return MinRetryDelay
	}
	// Full jitter over [MinRetryDelay, backoff]: a fleet of clients shed by the edge at the same
	// instant must not come back in lockstep, or the edge sheds them all again — and none of them
	// may come back at once, which is what the floor is for.
	return MinRetryDelay + time.Duration(mrand.Int63n(int64(backoff-MinRetryDelay)+1))
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
	// saw gets the original job back instead of paying for a second solve. Opaque. Leave it
	// empty and the SDK generates a UUIDv4 per Solve call (since 2026-10-20) so its own retries
	// of a 5xx that may already have landed cannot double-solve or double-charge; set it to
	// something that survives your process if you want to retry across restarts. SolveDeprecated
	// sends it too — yours, or one generated per call — and retries under the narrower rule
	// described there.
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
	// IdempotencyKey is the key the submit carried: SolveParams.IdempotencyKey when set, else the
	// one the SDK generated. Resubmitting with it returns this same job.
	IdempotencyKey string `json:"-"`
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
	// IdempotencyKey is sent as the Idempotency-Key header on the v2 submit; empty means the SDK
	// generates one per call, as for Solve. EvaluateDeprecated sends it too, generated when empty,
	// and retries under the same rule as SolveDeprecated.
	IdempotencyKey string
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
	retry   RetryPolicy
}

// Option configures a Client.
type Option func(*Client)

// WithBaseURL overrides the API base URL.
func WithBaseURL(u string) Option { return func(c *Client) { c.baseURL = strings.TrimRight(u, "/") } }

// WithHTTPClient supplies a custom *http.Client (e.g. a different timeout).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// WithRetryPolicy replaces the default retry policy. Zero durations keep their defaults, so
// RetryPolicy{MaxAttempts: 3} is a complete policy.
func WithRetryPolicy(p RetryPolicy) Option { return func(c *Client) { c.retry = p.withDefaults() } }

// WithoutRetries turns transient-failure retries off: the first 429, 5xx or transport error is
// returned as it was before 2026-10-20.
func WithoutRetries() Option { return func(c *Client) { c.retry.Disabled = true } }

// New creates a client. Panics if apiKey is empty.
func New(apiKey string, opts ...Option) *Client {
	if apiKey == "" {
		panic("kagedcap: apiKey is required")
	}
	c := &Client{apiKey: apiKey, baseURL: DefaultBaseURL, http: &http.Client{Timeout: 120 * time.Second}, retry: DefaultRetryPolicy()}
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
	err := c.withRetries(ctx, retryAsync, "polling solve "+id, func(ctx context.Context) error {
		return c.request(ctx, http.MethodGet, "/v2/solve/"+id, nil, &job)
	})
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// SolveDeprecated solves a captcha over the legacy synchronous /solve endpoint, which holds
// the HTTP connection open for the whole solve. Behaviour is exactly what Solve did before
// v2; CallbackURL, Deadline and PollInterval do not apply and are ignored — the client's own
// http.Client timeout bounds each request.
//
// Every call carries an Idempotency-Key — IdempotencyKey when set, else a UUIDv4 minted once per
// call — resent unchanged on every retry, so a bare call is no longer single-shot: a retry of a
// request the gateway may already be solving lands on that solve instead of a second, billable
// one. Only failures that never reached the gateway are retried — transport errors and the edge's
// own non-JSON 429/502/503/504 pages. A 429/5xx carrying the gateway's JSON envelope is final: the
// gateway claims the key before it does anything else, so resending would only answer 409
// idempotency_conflict (and a retry that does meet that 409 reports the failure before it). No
// retry is started more than 100s after the call's first attempt went out, because the gateway's
// claim on the key lives 120s and a later resend would run — and bill — a fresh solve rather than
// be deduplicated; the run then reports its last real response with the attempt count and the
// elapsed time, as it does at the deadline. The retry run on a context without a deadline
// (SolveDeprecated itself) gets DefaultSolveDeadline as a ceiling; pass your own through
// SolveDeprecatedContext to change it.
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
	if err := c.syncSolve(ctx, solveBody(p, taskFor(p)), p.IdempotencyKey, &out); err != nil {
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
	sub, err := c.submitRaw(ctx, evaluateBody(p), p.IdempotencyKey)
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

// EvaluateDeprecated runs evaluate over the legacy synchronous /solve endpoint. It always carries
// an Idempotency-Key — IdempotencyKey when set, else one minted per call — and retries under the
// same rule as SolveDeprecated.
//
// Deprecated: use Evaluate, which submits to /v2/solve and polls for the result.
func (c *Client) EvaluateDeprecated(p EvaluateParams) (*EvaluateResult, error) {
	var out EvaluateResult
	if err := c.syncSolve(context.Background(), evaluateBody(p), p.IdempotencyKey, &out); err != nil {
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
// KasadaParams has no key field, so the request goes out exactly once: without an Idempotency-Key
// a retry of a request the gateway may already be solving would be a second, billable solve.
func (c *Client) kasadaSync(ctx context.Context, body map[string]any) (*KasadaResult, error) {
	var out KasadaResult
	err := oneAttempt(ctx, func(ctx context.Context) error {
		return c.request(ctx, http.MethodPost, "/solve", body, &out)
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// syncSolve POSTs a body to the legacy synchronous /solve under an Idempotency-Key — the caller's,
// or a UUIDv4 minted here, exactly as submitRaw does — resent unchanged on every retry, so the
// gateway dedupes a resend of a request it is already solving. Only a failure that never reached
// it is retried (see isTransient), and none past legacyRetryWindow (see withRetries).
func (c *Client) syncSolve(ctx context.Context, body map[string]any, idempotencyKey string, out any) error {
	if idempotencyKey == "" {
		idempotencyKey = newIdempotencyKey()
	}
	err := c.withRetries(ctx, retrySync, "on the synchronous /solve", func(ctx context.Context) error {
		return c.request(ctx, http.MethodPost, "/solve", body, out, withIdempotencyKey(idempotencyKey))
	})
	return tagIdempotencyKey(err, idempotencyKey)
}

// submitRaw POSTs a prepared body to /v2/solve and returns the acknowledgement. The submit
// always carries an Idempotency-Key — the caller's, or a UUIDv4 minted here — because a retry
// of a 5xx/timeout the gateway may already have accepted must land on the same job, not a
// second billable one. Every retry resends the same key (2026-10-20).
func (c *Client) submitRaw(ctx context.Context, body map[string]any, idempotencyKey string) (*Submission, error) {
	if idempotencyKey == "" {
		idempotencyKey = newIdempotencyKey()
	}
	var sub Submission
	err := c.withRetries(ctx, retryAsync, "submitting to /v2/solve", func(ctx context.Context) error {
		return c.request(ctx, http.MethodPost, "/v2/solve", body, &sub, withIdempotencyKey(idempotencyKey))
	})
	if err != nil {
		return nil, tagIdempotencyKey(err, idempotencyKey)
	}
	if sub.ID == "" {
		// The submit answered 202 without an id, so there is nothing to poll and no way to find
		// the job again. Gateway bug, not the caller's — name it as one.
		return nil, &Error{Code: "internal_error", Message: "/v2/solve accepted the job but returned no id", IdempotencyKey: idempotencyKey}
	}
	sub.IdempotencyKey = idempotencyKey
	return &sub, nil
}

// retryKind says which endpoint a retry loop guards: the same 5xx reads differently on each
// (see isTransient).
type retryKind int

const (
	retryAsync retryKind = iota // POST /v2/solve and GET /v2/solve/{id}
	retrySync                   // POST /solve carrying an Idempotency-Key
)

// terminalCodes are API errors that say the request — or the key behind it — is wrong, whatever
// the status: the same request again gets the same answer, so a retry only burns the deadline.
// key_frozen is a 429 that does not thaw within any deadline; the subscription and host codes
// arrive as 402/403 today and are listed so a change of status could not make them retryable.
var terminalCodes = map[string]bool{
	"validation_error": true, "unauthorized": true, "key_revoked": true, "account_suspended": true,
	"insufficient_funds": true, "key_spend_cap_reached": true, "proxy_invalid": true, "proxy_required": true,
	"proxy_not_allowed": true, "unsupported_task": true, "action_required": true, "callback_url_invalid": true,
	"not_found": true, "idempotency_conflict": true, "key_frozen": true, "subscription_quota_exhausted": true,
	"subscription_past_due": true, "host_not_allowed": true,
}

// isTransient reports whether a failure is worth retrying.
//
// On the v2 endpoints: a transport error, or HTTP 429 / 500 / 502 / 503 / 504 with a code that
// does not condemn the request. Every other status (400, 401, 402, 403, 404, 405, 409, 413, 422,
// ...) is a verdict on the request and comes straight back. A 5xx on the v2 submit is retried
// whatever its code — even solve_failed — because the verdict on a v2 job never arrives on the
// submit response; it comes through the poll body as status "failed", which poll treats as terminal.
//
// On the legacy synchronous /solve the rule is narrower: only a failure that never reached the
// gateway — a transport error, or a 429/502/503/504 without our JSON envelope, which is what the
// Railway/Cloudflare edge sends when it sheds or cannot reach the app. The gateway claims the
// Idempotency-Key (SET NX EX 120) before it does anything else, so once a 429/5xx envelope has
// come back the same key can only answer 409 idempotency_conflict; the first answer is the final one.
func isTransient(e *Error, kind retryKind) bool {
	if e.Status == 0 {
		// Only transport failures; encode_error/request_error are the SDK's own and never clear up.
		return e.Code == "network_error"
	}
	if kind == retrySync {
		switch e.Status {
		case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return !e.envelope
		}
		return false
	}
	switch e.Status {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return !terminalCodes[e.Code]
	}
	return false
}

// withRetries runs attempt until it succeeds, fails terminally, or the budget runs out: the
// context's deadline, or RetryPolicy.MaxAttempts. A context without a deadline (SubmitSolve or
// GetSolve on context.Background()) gets DefaultSolveDeadline, so no caller retries forever
// against a gateway that never recovers. Between tries it waits RetryPolicy.Delay.
//
// The first attempt always goes out. A retry is started only when, after its wait, at least
// minAttemptBudget of the deadline would remain — a wait that could not leave that much is not
// served at all, since the outcome is already known. The error the run ends on is the last real
// answer, with its Code/Status/RequestID kept and a Message that says how many tries were made
// and how long they took — so a solve that died of rate limiting is reported as rate limiting,
// not as a bare timeout. A request the deadline itself cut short is never that last answer. A
// context cancelled by the caller ends the run with code "canceled" whatever came before.
//
// On the legacy /solve a retry is also never started more than legacyRetryWindow after the first
// attempt went out: the gateway's claim on the Idempotency-Key lives 120s, and a resend after it
// lapsed would run (and bill) a fresh solve instead of being deduplicated. That stop reports the
// last real response, with the attempt count and the elapsed time, exactly as the deadline does.
func (c *Client) withRetries(ctx context.Context, kind retryKind, where string, attempt func(context.Context) error) error {
	policy := c.retry
	if policy.Disabled {
		return oneAttempt(ctx, attempt)
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultSolveDeadline)
		defer cancel()
	}
	deadline, _ := ctx.Deadline()
	start := time.Now()
	var last *Error
	for n := 1; ; n++ {
		err := attempt(ctx)
		if err == nil {
			return nil
		}
		var e *Error
		if !errors.As(err, &e) {
			return err // a 2xx whose body would not decode; not a transient condition
		}
		if kind == retrySync && e.Code == "idempotency_conflict" && last != nil {
			return conflictAfterRetry(where, last, n)
		}
		if !isTransient(e, kind) {
			e.Attempts = n
			return err
		}
		if e.Status != 0 || ctx.Err() == nil {
			// A real answer, or a transport failure of its own. A request the context cut short
			// is not the run's last word; the response before it is.
			last = e
		}
		if ctx.Err() != nil {
			return ended(ctx, where, last, n, start)
		}
		if policy.MaxAttempts > 0 && n >= policy.MaxAttempts {
			return gaveUp(where, last, n, time.Since(start))
		}
		delay := policy.Delay(n, e.RetryAfter)
		if time.Until(deadline)-delay < minAttemptBudget {
			return gaveUp(where, last, n, time.Since(start))
		}
		if kind == retrySync && time.Since(start)+delay > legacyRetryWindow {
			// The retry would go out after the gateway's claim on the key has lapsed, so it could
			// not be deduplicated; the wait is not served, since the outcome is already known.
			return gaveUp(where, last, n, time.Since(start))
		}
		if !sleepCtx(ctx, delay) {
			return ended(ctx, where, last, n, start)
		}
	}
}

// oneAttempt sends a request exactly once — retries off, or a Kasada /solve, which carries no key —
// and stamps its error with the attempt count, so Attempts reads the same whichever path answered.
func oneAttempt(ctx context.Context, attempt func(context.Context) error) error {
	err := attempt(ctx)
	var e *Error
	if errors.As(err, &e) {
		e.Attempts = 1
	}
	return err
}

// ended is the error for a retry run the context stopped: a cancellation is reported as one,
// whatever the responses before it said; a deadline reports the last real response, or a plain
// timeout when the deadline cut the only request short.
func ended(ctx context.Context, where string, last *Error, attempts int, start time.Time) *Error {
	if ctx.Err() == context.Canceled {
		msg := fmt.Sprintf("context canceled %s after %s", where, plural(attempts, "attempt"))
		if last != nil {
			msg += "; last response: " + last.describe()
		}
		return &Error{Code: "canceled", Message: msg, Attempts: attempts}
	}
	if last == nil {
		return &Error{Code: "solve_timeout", Message: "deadline exceeded " + where, Attempts: attempts}
	}
	return gaveUp(where, last, attempts, time.Since(start))
}

// gaveUp is the error a retry run ends on: the last answer's identity, plus the run's story.
func gaveUp(where string, last *Error, attempts int, waited time.Duration) *Error {
	return withLast(last, attempts, fmt.Sprintf("gave up %s after %s over %s; last response: %s",
		where, plural(attempts, "attempt"), waited.Round(time.Millisecond), last.describe()))
}

// conflictAfterRetry is the error for a legacy /solve retry the gateway answered with 409
// idempotency_conflict: it proves an earlier attempt did reach the gateway, so the honest outcome
// is that attempt's failure, not the conflict.
func conflictAfterRetry(where string, last *Error, attempts int) *Error {
	return withLast(last, attempts, fmt.Sprintf("gave up %s after %s: the retry answered 409 idempotency_conflict, so an earlier attempt reached the gateway; last response: %s",
		where, plural(attempts, "attempt"), last.describe()))
}

// withLast builds the error a retry run reports: last's identity under a message of the run's own.
func withLast(last *Error, attempts int, message string) *Error {
	return &Error{
		Status:     last.Status,
		Code:       last.Code,
		RequestID:  last.RequestID,
		RetryAfter: last.RetryAfter,
		Attempts:   attempts,
		Message:    message,
	}
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

// sleepCtx waits d or until ctx ends, reporting whether the full wait was served.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// tagIdempotencyKey records the key a failed submit or legacy /solve carried on the error, so the
// caller can find the job the gateway may already hold under it.
func tagIdempotencyKey(err error, key string) error {
	var e *Error
	if errors.As(err, &e) {
		e.IdempotencyKey = key
	}
	return err
}

// newIdempotencyKey mints a random UUIDv4 (RFC 4122) from crypto/rand, stdlib only. The
// fallback is for a broken entropy source, which does not happen on any supported platform.
func newIdempotencyKey() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		for i := range b {
			b[i] = byte(mrand.Intn(256))
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// parseRetryAfter reads a Retry-After header: delay-seconds or an HTTP-date. Unparseable, zero,
// negative, past, or absent is zero, which means "use the backoff"; anything beyond maxRetryAfter
// is clamped to it before it becomes a Duration, so the result is never negative or wrapped.
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil || errors.Is(err, strconv.ErrRange) {
		// ParseInt saturates on ErrRange, so an overflowing count keeps its sign: a huge positive
		// one lands on the cap, a huge negative one on zero.
		if secs <= 0 {
			return 0
		}
		if secs > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		return time.Duration(secs) * time.Second
	}
	at, err := http.ParseTime(v)
	if err != nil || at.Year() < 1970 || at.Year() > 9999 {
		return 0
	}
	now := time.Now()
	if !at.After(now) {
		return 0
	}
	if at.After(now.Add(maxRetryAfter)) {
		return maxRetryAfter
	}
	return at.Sub(now)
}

// poll loops GET /v2/solve/{id} every interval until the job is done (returns the Job), failed
// (returns the gateway's error code) or ctx runs out. An unfamiliar status means keep waiting:
// treating one as terminal would break the SDK the day the gateway adds one. A poll that fails
// transiently (429, 5xx, transport) is retried inside GetSolveContext with the retry backoff;
// interval only paces the polls that answered.
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
			// Transient failures were already retried inside GetSolveContext, so what arrives
			// here is terminal: not_found (404, which the gateway also returns for a job belonging
			// to another account — the id will never resolve), another verdict on the request, or
			// a retry run that exhausted the deadline. Stop rather than burn what is left of it.
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
			// The controller returns the exact field issues on a 400
			// validation_error. Dropping them left the caller with the bare
			// code and nothing to act on: the gateway knew which field was
			// wrong and the SDK threw the answer away.
			Issues []struct {
				Path    string `json:"path"`
				Message string `json:"message"`
			} `json:"issues"`
		}
		_ = json.Unmarshal(data, &e)
		// Our envelope always names an error; the Railway/Cloudflare edge's own 429/5xx pages are
		// plain text or HTML (Railway's 502 is JSON, but keyed on status/code/message, not error).
		envelope := e.Error != ""
		if !envelope {
			e.Error = "error"
		}
		if e.Message == "" {
			e.Message = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
		// Append the field issues to the message rather than adding a field
		// nobody reads: every caller already prints Error(), so this is the one
		// place a validation_error can become actionable without each of them
		// changing. "validation_error" alone sent a real debugging session
		// hunting the whole body.
		if len(e.Issues) > 0 {
			parts := make([]string, 0, len(e.Issues))
			for _, iss := range e.Issues {
				switch {
				case iss.Path != "" && iss.Message != "":
					parts = append(parts, iss.Path+": "+iss.Message)
				case iss.Path != "":
					parts = append(parts, iss.Path)
				case iss.Message != "":
					parts = append(parts, iss.Message)
				}
			}
			if len(parts) > 0 {
				e.Message += " (" + strings.Join(parts, "; ") + ")"
			}
		}
		return &Error{Status: resp.StatusCode, Code: e.Error, Message: e.Message, RequestID: e.RequestID,
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")), envelope: envelope}
	}
	return json.Unmarshal(data, out)
}

// withIdempotencyKey sets the Idempotency-Key header. The v2 submit and the legacy /solve both
// always carry one (the caller's, or a generated UUIDv4); Kasada sends none and never comes here.
func withIdempotencyKey(key string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("Idempotency-Key", key) }
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
	body := map[string]any{"task": task, "url": p.URL}
	// Omitted when empty, like action and for the same reason: the gateway's schema is strict
	// and types sitekey as a non-empty optional string, so a literal "" is a validation_error
	// rather than an absent field. That only ever bites the tasks whose sitekey lives
	// server-side — tmpt and evaluate — which have no sitekey to send and were rejected for
	// sending the empty one. reCAPTCHA is unaffected: it must send a sitekey either way, and
	// a caller that omits one is still refused, just by name instead of by length.
	putIf(body, "sitekey", p.Sitekey)
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

// solveWaitErr keeps the poll loop speaking the SDK's error type: a context that ran out between
// polls is a solve_timeout whichever deadline fired, the caller's own or the per-call Deadline,
// and a cancelled one is canceled. Anything that went through a retry run travels untouched —
// it already names the last real status and the attempt count (or canceled / solve_timeout with
// the count), and says more than a bare timeout would.
func solveWaitErr(ctx context.Context, where string, err error) error {
	var e *Error
	if errors.As(err, &e) && e.Attempts > 0 {
		return err
	}
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
