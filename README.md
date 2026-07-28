<p align="center"><img src="https://kagedcap.io/kc-logo.svg" width="260" alt="KagedCap"></p>

# KagedCap Go SDK

Solve reCAPTCHA (v3, v3 Enterprise, v2), Ticketmaster tmpt, and Kasada with a single API
key. Standard library only — no dependencies.

## Install

```bash
go get github.com/kagedcap/kagedcap-sdk-go
```

Requires Go 1.21+.

## Quick start

```go
package main

import (
	"fmt"
	"log"
	"os"

	kagedcap "github.com/kagedcap/kagedcap-sdk-go"
)

func main() {
	kc := kagedcap.New(os.Getenv("KAGEDCAP_API_KEY"))

	res, err := kc.Solve(kagedcap.SolveParams{
		Sitekey:    "6LcvL3UrAAAAAO_9u8Seiuf-I6F_tP_jSS-zndXV",
		URL:        "https://www.ticketmaster.com",
		Action:     "Event",
		// Send a real desktop UA — the token embeds it, so match the browser your traffic presents.
		UserAgent:  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
		Enterprise: true, // ProxyLess Enterprise
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res.Token)

	bal, _ := kc.CheckBalance()
	fmt.Println("balance:", bal.Display)
}
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

## Errors

Failures return `*kagedcap.Error` with `.Status`, `.Code`, and `.Message`:

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

Only successful solves are billed.
