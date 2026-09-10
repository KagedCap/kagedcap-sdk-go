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
| `IdempotencyKey` | unset | Sent as the `Idempotency-Key` header on the submit. The gateway dedupes on it durably and across shards, so retrying a submit you never saw the answer to returns the original job instead of paying for a second solve |

A successful result adds `SolveMS` and `ElapsedMS` — how long the solver took, and the wall
clock from submit to completion. Both are `*float64` because the gateway may report neither;
`nil` means "not reported", never zero. `Score` and `Verification` are **not** in the v2 poll
response and stay `nil` on this path — only `SolveDeprecated` fills them in.

Failures come back as `*kagedcap.Error`, the same type `Solve` has always returned:

| Code | When |
| --- | --- |
| `solve_timeout` | `Deadline` (or the context's) ran out while polling |
| `canceled` | the context was cancelled |
| the gateway's own code | the job reported `failed` — its `error` is passed through, e.g. `proxy_required`; `solve_failed` when it gives no reason |
| `result_expired` | the job finished but its token was already cleared — see below |
| `not_found` | the id is unknown, or belongs to another account |

Results are kept for about five minutes after a solve completes, then the token is cleared
(a reCAPTCHA token is dead inside two minutes anyway). A poll that arrives after that still
says `done` but carries no token, and `Solve` reports it as `result_expired` rather than
handing you an empty string that looks like success.

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

- `Score` and `Verification` are always `nil` from `Solve` now; the v2 poll response has no
  equivalent. Use `SolveDeprecated` if you branch on them.
- A failed job surfaces the gateway's own error code rather than a blanket `solve_failed`.

`SolveParams` and `SolveResult` gained fields but lost none, and `Kasada*`, `Evaluate`, and
`CheckBalance` are untouched.

`SolveDeprecated` (and `SolveDeprecatedContext`) call the legacy synchronous `/solve`
endpoint, which holds the HTTP connection open for the whole solve. Behaviour is exactly
v1's `Solve`, so it is the one-line escape hatch while you migrate — but it is going away,
and it ignores `Deadline`, `PollInterval`, `CallbackURL`, and `IdempotencyKey`. The only
bound on it is the client's own `http.Client` timeout.

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

Common codes: `unauthorized`, `insufficient_funds`, `solve_failed`, `solve_timeout`,
`proxy_required`, `proxy_not_allowed`, `validation_error`, `concurrency_limit_exceeded`, `key_frozen`.
A submit can also fail with `callback_url_invalid`, `maintenance`, or `proxyless_disabled`.

`solve_timeout`, `canceled`, and `result_expired` are raised by `Solve` itself rather than by
the API, so they carry no `.Status` or `.RequestID`.

Only successful solves are billed.
