<p align="center"><img src="https://kagedcap.io/kc-logo.svg" width="260" alt="KagedCap"></p>

# KagedCap Go SDK

Solve reCAPTCHA (v3, v3 Enterprise, v2), Ticketmaster tmpt, Kasada, and Ticketmaster
evaluate with a single API key. Standard library only — no dependencies.

## Install

```bash
go get github.com/kagedcap/kagedcap-sdk-go/v2
```

Requires Go 1.21+. v2 changes how `Solve` talks to the API — see
[Upgrading to v2](#upgrading-to-v2).

## Quick start

```go
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	kagedcap "github.com/kagedcap/kagedcap-sdk-go/v2"
)

func main() {
	kc := kagedcap.New(os.Getenv("KAGEDCAP_API_KEY"))

	res, err := kc.Solve(kagedcap.SolveParams{
		Sitekey:    "6LcvL3UrAAAAAO_9u8Seiuf-I6F_tP_jSS-zndXV",
		URL:        "https://www.ticketmaster.com",
		Action:     "Event",
		// UserAgent omitted — the SDK sends kagedcap.DefaultUserAgent, the same Chrome desktop
		// profile the solver runs. Set it to match the browser your own traffic presents.
		Enterprise: true,             // ProxyLess Enterprise
		Deadline:   90 * time.Second, // give up after this; unset means 120s
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res.Token)

	bal, _ := kc.CheckBalance()
	fmt.Println("balance:", bal.Display)
}
```

`Solve` still blocks and still hands you a token. Underneath it now submits the job to
`POST /v2/solve` (which answers `202` with a job id) and polls `GET /v2/solve/{id}` until the
job reports `done`, so no request is held open for the length of a solve.

| Field | Default | Meaning |
| --- | --- | --- |
| `Deadline` | `120 * time.Second` | Whole budget: the submit plus every poll after it |
| `PollInterval` | `5 * time.Second` | Wait between status polls |
| `CallbackURL` | unset | Sent as `callback_url` — https and publicly resolvable. The gateway posts the finished solve to you as well; `Solve` polls either way |
| `IdempotencyKey` | a UUIDv4 per call | Sent as the `Idempotency-Key` header on the submit and on every retry of it (and on `SolveDeprecated`'s `/solve`). The gateway dedupes on it durably and across shards, so retrying a submit you never saw the answer to returns the original job instead of paying for a second solve. Set your own if you want to retry across process restarts; see [Retries](#retries) |

A successful result adds `SolveMS` and `ElapsedMS` — how long the solver took, and the wall
clock from submit to completion. Both are `*float64` because the gateway may report neither;
`nil` means "not reported", never zero. `Score` and `Verification` are reCAPTCHA-only and come
back from the poll when the gateway has them; they stay `nil` for tmpt and whenever not reported.

Failures come back as `*kagedcap.Error`, the same type `Solve` has always returned:

| Code | When |
| --- | --- |
| `solve_timeout` | `Deadline` (or the context's) ran out while polling a job that was still running |
| `canceled` | the context was cancelled — between polls, or during a retry's backoff; `.Attempts` says how many requests went out |
| the gateway's own code | the job reported `failed` — its `error` is passed through, e.g. `proxy_unreachable`; `solve_failed` when it gives no reason |
| the last response's code, e.g. `rate_limited` | `Deadline` ran out while the SDK was retrying 429/5xx/transport failures; `.Status` and `.Attempts` say what it was up against — see [Retries](#retries) |
| `result_expired` | the job finished but its token was already cleared — see below |
| `not_found` | the id is unknown, or belongs to another account |

Results are kept for about five minutes after a solve completes, then the token is cleared
(a reCAPTCHA token is dead inside two minutes anyway). A poll that arrives after that still
says `done` but carries no token, and `Solve` reports it as `result_expired` rather than
handing you an empty string that looks like success.

## Retries

Since 2026-10-20 the SDK retries transient failures on its own instead of failing the solve
on the first one. The edge in front of the gateway sheds bursts of submits and polls with a
bare `429` before they reach the API, and the API itself answers `429` / `503` under load; those
requests succeed a moment later.

- **What is retried:** HTTP `429`, `500`, `502`, `503`, `504` and transport errors (connection
  refused/reset, EOF, timeouts, DNS) on the v2 submit (`POST /v2/solve`) and the status poll
  (`GET /v2/solve/{id}`). `GetSolve` and `SubmitSolve` retry too, bounded by `DefaultSolveDeadline`
  when their context has no deadline.
- **What is not:** every other status (`400`, `401`, `402`, `403`, `404`, `409`, `422`, ...), a
  job that reports `failed`, and codes that mean the request or the key is wrong, whatever status
  they arrive on: `validation_error`, `unauthorized`, `key_revoked`, `account_suspended`,
  `insufficient_funds`, `key_spend_cap_reached`, `proxy_invalid`, `proxy_required`,
  `proxy_not_allowed`, `unsupported_task`, `action_required`, `callback_url_invalid`, `not_found`,
  `idempotency_conflict`, `key_frozen`, `subscription_quota_exhausted`, `subscription_past_due`,
  `host_not_allowed`. On the v2 submit the verdict only ever arrives through the poll, so a `5xx`
  there is retried even when it says `solve_failed`.
- **The legacy `/solve`** (`SolveDeprecated`, `EvaluateDeprecated`) always carries an
  `Idempotency-Key` too — `IdempotencyKey` if set, else a UUIDv4 minted once per call — resent
  unchanged on every retry, so a bare call is no longer single-shot. It is retried only on failures
  that never reached the gateway: transport errors and the edge's own non-JSON `429` / `502` /
  `503` / `504` pages. A `429` or `5xx` carrying the gateway's JSON envelope (`no_capacity`,
  `overloaded`, `solve_failed`, ...) is final: the gateway claims the key before it does anything
  else, so a resend could only answer `409 idempotency_conflict`. If a retry does meet that `409`,
  the error you get is the failure that preceded it (with `.Attempts`), not the conflict. No retry
  is started more than 100s after the call's first request went out: the gateway's claim on the
  key lives 120s, and a resend after it lapsed would run — and bill — a fresh solve instead of
  being deduplicated. That stop reports the last real response with `.Attempts` and the elapsed
  time, exactly as the deadline does. Kasada carries no key and is sent once.
- **Backoff:** 250ms, doubling, capped at 5s, with full jitter (uniform in `[50ms, backoff]`). A
  `Retry-After` header (seconds or HTTP-date) is honoured instead, capped at 10s. No wait is ever
  shorter than 50ms (`MinRetryDelay`) — not for a `Retry-After: 0`, not for an unparseable one,
  and not for a policy configured with a zero or negative backoff, which means the default. The
  `.RetryAfter` exposed on an error is clamped to 24h and is never negative, whatever the header
  said. The poll interval still paces polls that answered.
- **The deadline:** retries stop at `Deadline` / the context, and a retry is never started with
  less than 500ms of it left — the run reports its last real response instead of burning the
  rest on a try that could not answer in time. The error keeps that response's `.Code`, `.Status`
  and `.RequestID` (never a synthetic `solve_timeout`, and never the request the deadline itself
  cut short), and its `.Message` names the attempt count and the total wait, e.g.
  `gave up submitting to /v2/solve after 23 attempts over 30s; last response: HTTP 429 error: HTTP 429`.
  `.Attempts` carries the count.
- **Cancellation:** cancelling the context during a backoff returns `canceled`, with `.Attempts`
  and the last response in `.Message`, whatever the responses before it said.
- **Idempotency:** every v2 submit carries an `Idempotency-Key` — `SolveParams.IdempotencyKey`
  if set, else a UUIDv4 minted per `Solve` call — and every retry resends the same key, so a
  submit the gateway did accept before a `5xx` is never solved or billed twice. The key is on
  `Submission.IdempotencyKey` and on `Error.IdempotencyKey` of a failed submit.

Turn it off, or tune it, per client:

```go
kc := kagedcap.New(key, kagedcap.WithoutRetries()) // every 429/5xx is returned at once, as before

kc := kagedcap.New(key, kagedcap.WithRetryPolicy(kagedcap.RetryPolicy{
	MaxAttempts:    5,                      // 0 (default) means until the deadline
	InitialBackoff: 500 * time.Millisecond, // defaults: 250ms → 5s, Retry-After capped at 10s
	MaxBackoff:     8 * time.Second,
}))
```

`RetryPolicy.Delay(failures, retryAfter)` is exported if you drive `SubmitSolve` / `GetSolve`
by hand and want the same pacing.

## Cancellation

`SolveContext` takes a `context.Context` and the poll loop honours it. Whichever runs out
first — the context's deadline or `Deadline` — ends the wait; cancelling the context stops
polling with code `canceled`.

```go
ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
defer cancel()

res, err := kc.SolveContext(ctx, kagedcap.SolveParams{
	Sitekey: "6Lc...",
	URL:     "https://example.com/login",
	Action:  "login",
})
```

## Upgrading to v2

The import path gains `/v2` and `Solve` moves to the submit-and-poll endpoints described
above. Its signature is unchanged — the same params in, the same `*SolveResult` out — so most
callers only edit the import line. Two things to check:

- A failed job surfaces the gateway's own error code rather than a blanket `solve_failed`.
- `Evaluate` now submits to `/v2/solve` and polls as well — same signature and result type,
  just no request held open.
- `KasadaLogin` and `KasadaReload` POST to the synchronous `/solve` endpoint and return the
  full result on the one request; they do not use the async submit-and-poll path. Same
  signatures and result types as before.
- The `*Deprecated` variants (`KasadaLoginDeprecated`, `KasadaReloadDeprecated`,
  `EvaluateDeprecated`) still call the legacy `/solve`; for Kasada they now behave exactly like
  the plain methods.
- New primitives `SubmitSolve` and `GetSolve` expose the submit and poll steps directly, for a
  callback-driven flow or your own loop.

`SolveParams` and `SolveResult` gained fields but lost none, and `CheckBalance` is untouched.

`SolveDeprecated` (and `SolveDeprecatedContext`) call the legacy synchronous `/solve`
endpoint, which holds the HTTP connection open for the whole solve. Behaviour is exactly
v1's `Solve`, so it is the one-line escape hatch while you migrate — but it is going away,
and it ignores `Deadline`, `PollInterval`, and `CallbackURL`. Each request is bounded by the
client's own `http.Client` timeout. Every call carries an `Idempotency-Key` — `IdempotencyKey`
if set, else a UUIDv4 minted per call — so failures that never reached the gateway are retried
under it (see [Retries](#retries)); a bare call is no longer single-shot. That retry run gets a
120s ceiling (`DefaultSolveDeadline`) when the context has no deadline of its own — pass one
through `SolveDeprecatedContext` to change it — and never starts a retry more than 100s after
its first request, the gateway's claim on the key being good for 120s.

```go
res, err := kc.SolveDeprecated(params) // legacy blocking /solve
```

## With a proxy

```go
res, err := kc.Solve(kagedcap.SolveParams{
	Sitekey: "6Lc...",
	URL:     "https://example.com/login",
	Action:  "login",
	Proxy:   "http://user:pass@1.2.3.4:8080", // or host:port:user:pass
})
```

Leave `Proxy` empty for a ProxyLess solve. Set `Enterprise: true` for Enterprise
sitekeys, or set `Task` explicitly.

## User agent

The token embeds the UA, so every solve should carry one. Leave `UserAgent` empty and the
SDK sends `kagedcap.DefaultUserAgent`; pass your own to override it:

```go
res, err := kc.Solve(kagedcap.SolveParams{
	Sitekey:   "6Lc...",
	URL:       "https://example.com/login",
	Action:    "login",
	UserAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ...", // match your traffic
})
```

Kasada is the exception: `KasadaLogin` and `KasadaReload` never send a UA — the harvester
picks the identity and hands it back in `Headers` and `UserAgent`.

## Kasada

`KasadaLogin` starts a session (requires a proxy — the token is IP-bound) and returns the
full header set. Keep that result and pass it to `KasadaReload` to refresh the session — the
SDK resends the session's `KpsdkST`, `Hash`, and `XKpsdk*` values for you (`Hash` + `XKpsdkCt` are required).

```go
login, err := kc.KasadaLogin(kagedcap.KasadaParams{
	Site:  "ticketmaster",
	Proxy: "http://user:pass@1.2.3.4:8080",
})
if err != nil {
	log.Fatal(err)
}
// Replay login.Headers (user-agent + sec-ch-ua*) and login.XKpsdk* on your request.

fresh, err := kc.KasadaReload(login) // no proxy needed
if err != nil {
	log.Fatal(err)
}
fmt.Println(fresh.XKpsdkCd)
```

Kasada results have **no `Token`** — replay `Headers` and the `XKpsdk*` values instead.

`KasadaLogin` and `KasadaReload` POST to the synchronous `/solve` endpoint and hand back the
full result on the one request — no submit-and-poll, so a request is held open for the length
of the solve. `KasadaLoginDeprecated` / `KasadaReloadDeprecated` call the same `/solve` and now
behave identically.

## Evaluate

`Evaluate` runs Ticketmaster's EPSF step and returns the allow token to replay on the next
APS request. `URL` and `Proxy` are required.

```go
ev, err := kc.Evaluate(kagedcap.EvaluateParams{
	URL:         "https://auth.ticketmaster.com/epsf/gec/",
	Proxy:       "http://user:pass@1.2.3.4:8080",
	PhoneNumber: "+12025550123", // verify_phone only
})
if err != nil {
	log.Fatal(err)
}
fmt.Println(ev.Token, ev.Decision) // decision: allow | challenge | block
```

`Action` is derived from `URL` — `auth.*` hosts verify a phone, any other Ticketmaster host
joins a queue. Leave it empty to keep that default, or set it to `"verify_phone"` /
`"join_queue"` to override. For a queue join, pass `QueueID` and `EventID` instead of
`PhoneNumber`:

```go
ev, err := kc.Evaluate(kagedcap.EvaluateParams{
	URL:     "https://queue.ticketmaster.com/...",
	Proxy:   "http://user:pass@1.2.3.4:8080",
	QueueID: "...",
	EventID: "...",
})
```

Unset fields are omitted from the request. `UserAgent` follows the same rule as `Solve` —
empty sends `kagedcap.DefaultUserAgent` — and it picks the solver's device profile, not just
a header, so match it to your own traffic.

`Evaluate` submits to `/v2/solve` and polls for the result; `EvaluateDeprecated` uses the
legacy blocking `/solve`.

## Errors

Failures return `*kagedcap.Error` with `.Status`, `.Code`, `.Message`, and `.RequestID` —
quote the request id when reporting a failure:

```go
res, err := kc.Solve(params)
if err != nil {
	var kcErr *kagedcap.Error
	if errors.As(err, &kcErr) && kcErr.Code == "insufficient_funds" {
		// top up
	}
}
```

Every error from the gateway carries one of these stable `.Code` values:

| Code | HTTP | Meaning |
| --- | --- | --- |
| `validation_error` | 400 | bad request shape |
| `proxy_invalid` | 400 | proxy malformed or disallowed — caught pre-flight, no solve attempted |
| `callback_url_invalid` | 400 | `callback_url` wasn't a public https URL |
| `unauthorized` | 401 | missing or invalid API key |
| `insufficient_funds` | 402 | balance too low |
| `key_spend_cap_reached` | 402 | the key hit its spend cap |
| `subscription_quota_exhausted` | 402 | the solve package is out of quota |
| `subscription_past_due` | 402 | the subscription is past due |
| `account_suspended` | 403 | the account is suspended |
| `host_not_allowed` | 403 | the key's allowlist doesn't include the page host |
| `not_found` | 404 | a `/v2` job id that's unknown, expired, or not yours |
| `idempotency_conflict` | 409 | the `Idempotency-Key` was reused with a different request |
| `proxy_unreachable` | 422 | the solve ran and your proxy didn't answer — not billed (was `502` before 2026-09-24) |
| `concurrency_limit_exceeded` | 429 | too many solves in flight |
| `key_frozen` | 429 | key auto-frozen after a burst of failed solves (usually bad proxies) |
| `rate_limited` | 429 | request rate too high |
| `overloaded` | 429 | the gateway is shedding load |
| `internal_error` | 500 | an unexpected gateway error |
| `solve_failed` | 502 | the solve was attempted and failed — not billed |
| `no_capacity` | 503 | no solver capacity right now |
| `solver_unavailable` | 503 | a node was chosen but couldn't be reached |
| `maintenance` | 503 | the fleet is in maintenance |
| `proxyless_disabled` | 503 | proxyless solves are disabled |
| `solve_timeout` | 504 | the solve ran past its deadline — not billed |

The SDK retries `429`, `500`, `502`, `503` and `504` with backoff for you until `Deadline`
(see [Retries](#retries)); when one of them reaches you from the v2 endpoints it is because the
deadline ran out, and `.Attempts` says how many tries it took. On `SolveDeprecated` the gateway's
own `429`/`5xx` reaches you at once, since a resend of the same `Idempotency-Key` could only
answer `409`. A `422` `proxy_unreachable`, or a `502` `solve_failed` from `SolveDeprecated`,
means a real solve was attempted (and not billed), so fix the input rather than retrying
blindly — for `422` it's always the proxy.

`solve_timeout`, `canceled`, `network_error`, and `result_expired` can also be raised by the SDK
itself rather than by the API, so those carry no `.Status` or `.RequestID`; the SDK's `solve_timeout`
is the local `Deadline` firing, distinct from the gateway's `504`.

Only successful solves are billed.
