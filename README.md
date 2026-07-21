<p align="center"><img src="https://kagedcap.io/kc-logo.svg" width="260" alt="KagedCap"></p>

# KagedCap Go SDK

Solve reCAPTCHA v3 and v3 Enterprise tokens with a single API key. Standard library
only — no dependencies.

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
`proxy_required`, `proxy_not_allowed`, `validation_error`.

Only successful solves are billed.
