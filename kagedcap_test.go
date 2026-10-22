package kagedcap_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kagedcap "github.com/kagedcap/kagedcap-sdk-go/v2"
)

// newClient points a Client at a stand-in gateway. Every v2 test drives the real poll loop
// against it, so the contract these handlers encode is the thing under test.
func newClient(t *testing.T, h http.HandlerFunc, opts ...kagedcap.Option) *kagedcap.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return kagedcap.New("test-key", append([]kagedcap.Option{kagedcap.WithBaseURL(srv.URL)}, opts...)...)
}

// fastRetries pins the retry backoff to its 50ms floor so every wait is exactly MinRetryDelay and
// a test that provokes a dozen retries still finishes inside a second; the production 250ms→5s
// curve is covered by TestRetryDelayStaysUnderTheCapAndIsJittered against the policy directly.
func fastRetries() kagedcap.Option {
	return kagedcap.WithRetryPolicy(kagedcap.RetryPolicy{InitialBackoff: kagedcap.MinRetryDelay, MaxBackoff: kagedcap.MinRetryDelay})
}

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// params is a solve with the waits collapsed — the 5s production interval would make this
// file take minutes.
func params() kagedcap.SolveParams {
	return kagedcap.SolveParams{
		Sitekey:      "6Lc-test",
		URL:          "https://example.com/login",
		Action:       "login",
		PollInterval: 5 * time.Millisecond,
		Deadline:     2 * time.Second,
	}
}

func kcErr(t *testing.T, err error) *kagedcap.Error {
	t.Helper()
	if err == nil {
		t.Fatal("want an error, got nil")
	}
	var e *kagedcap.Error
	if !errors.As(err, &e) {
		t.Fatalf("want *kagedcap.Error, got %T: %v", err, err)
	}
	return e
}

func TestSolveReturnsTokenWhenStatusReachesDone(t *testing.T) {
	var polls atomic.Int32
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/solve":
			if got := r.Header.Get("x-api-key"); got != "test-key" {
				t.Errorf("x-api-key = %q, want test-key", got)
			}
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-1","status":"running"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v2/solve/job-1":
			// success is false for as long as the job runs; only status says whether to stop.
			if polls.Add(1) < 3 {
				io.WriteString(w, `{"success":false,"id":"job-1","status":"running","token":null,"created_at":"2026-09-10T00:00:00Z"}`)
				return
			}
			io.WriteString(w, `{"success":true,"id":"job-1","status":"done","token":"03AFcWeA","solve_ms":1840,"elapsed_ms":2110,"created_at":"2026-09-10T00:00:00Z","completed_at":"2026-09-10T00:00:02Z"}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})

	res, err := kc.Solve(params())
	if err != nil {
		t.Fatalf("Solve: %v", err)
	}
	if res.Token != "03AFcWeA" {
		t.Errorf("Token = %q, want 03AFcWeA", res.Token)
	}
	if !res.Success {
		t.Error("Success = false, want true")
	}
	if res.Task != "ReCaptchaV3TaskProxyLess" {
		t.Errorf("Task = %q, want the submitted task ReCaptchaV3TaskProxyLess", res.Task)
	}
	if res.SolveMS == nil || *res.SolveMS != 1840 {
		t.Errorf("SolveMS = %v, want 1840", res.SolveMS)
	}
	if res.ElapsedMS == nil || *res.ElapsedMS != 2110 {
		t.Errorf("ElapsedMS = %v, want 2110", res.ElapsedMS)
	}
	if got := polls.Load(); got != 3 {
		t.Errorf("polled %d times, want 3", got)
	}
}

func TestSolveSucceedsWhenTimingsAreAbsent(t *testing.T) {
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-1","status":"running"}`)
			return
		}
		// solve_ms null, elapsed_ms missing entirely — neither may be required.
		io.WriteString(w, `{"success":true,"id":"job-1","status":"done","token":"tok","solve_ms":null}`)
	})

	res, err := kc.Solve(params())
	if err != nil {
		t.Fatalf("Solve: %v", err)
	}
	if res.SolveMS != nil || res.ElapsedMS != nil {
		t.Errorf("timings = %v/%v, want nil/nil when the gateway omits them", res.SolveMS, res.ElapsedMS)
	}
}

func TestSolveReportsResultExpiredWhenDoneCarriesNoToken(t *testing.T) {
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-1","status":"running"}`)
			return
		}
		// The gateway clears token a few minutes after completion.
		io.WriteString(w, `{"success":true,"id":"job-1","status":"done","token":null,"completed_at":"2026-09-10T00:00:02Z"}`)
	})

	res, err := kc.Solve(params())
	if res != nil {
		t.Fatalf("want no result, got %+v", res)
	}
	if got := kcErr(t, err).Code; got != "result_expired" {
		t.Errorf("Code = %q, want result_expired", got)
	}
}

func TestSolveSurfacesTheGatewayErrorWhenTheJobFails(t *testing.T) {
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-1","status":"running"}`)
			return
		}
		io.WriteString(w, `{"success":false,"id":"job-1","status":"failed","token":null,"error":"proxy_required"}`)
	})

	_, err := kc.Solve(params())
	if got := kcErr(t, err).Code; got != "proxy_required" {
		t.Errorf("Code = %q, want the gateway's own proxy_required", got)
	}
}

func TestSolveStopsPollingWhenTheJobIsNotFound(t *testing.T) {
	var polls atomic.Int32
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-1","status":"running"}`)
			return
		}
		polls.Add(1)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"success":false,"error":"not_found","message":"No such solve","request_id":"req-77"}`)
	}, fastRetries()) // retries on, to prove a 404 is never one of them

	_, err := kc.Solve(params())
	e := kcErr(t, err)
	if e.Code != "not_found" || e.Status != http.StatusNotFound {
		t.Errorf("got code=%q status=%d, want not_found/404", e.Code, e.Status)
	}
	if e.RequestID != "req-77" {
		t.Errorf("RequestID = %q, want req-77", e.RequestID)
	}
	if got := polls.Load(); got != 1 {
		t.Errorf("polled %d times, want 1 — a 404 never resolves", got)
	}
}

func TestSolveTimesOutWhenTheDeadlineElapsesMidPoll(t *testing.T) {
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-1","status":"running"}`)
			return
		}
		io.WriteString(w, `{"success":false,"id":"job-1","status":"running","token":null}`)
	})

	p := params()
	p.Deadline = 60 * time.Millisecond
	_, err := kc.Solve(p)
	if got := kcErr(t, err).Code; got != "solve_timeout" {
		t.Errorf("Code = %q, want solve_timeout", got)
	}
}

func TestSolveContextStopsPollingWhenTheContextIsCanceled(t *testing.T) {
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-1","status":"running"}`)
			return
		}
		io.WriteString(w, `{"success":false,"id":"job-1","status":"running","token":null}`)
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	defer cancel()

	_, err := kc.SolveContext(ctx, params())
	if got := kcErr(t, err).Code; got != "canceled" {
		t.Errorf("Code = %q, want canceled", got)
	}
}

func TestSolveReturnsTheSubmitErrorEnvelope(t *testing.T) {
	var polls atomic.Int32
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			polls.Add(1)
		}
		w.WriteHeader(http.StatusPaymentRequired)
		io.WriteString(w, `{"success":false,"error":"insufficient_funds","message":"Balance too low","request_id":"req-42"}`)
	})

	_, err := kc.Solve(params())
	e := kcErr(t, err)
	if e.Code != "insufficient_funds" || e.Status != http.StatusPaymentRequired {
		t.Errorf("got code=%q status=%d, want insufficient_funds/402", e.Code, e.Status)
	}
	if e.RequestID != "req-42" {
		t.Errorf("RequestID = %q, want req-42", e.RequestID)
	}
	if got := polls.Load(); got != 0 {
		t.Errorf("polled %d times after a rejected submit, want 0", got)
	}
}

func TestSolveSendsCallbackURLAndIdempotencyKey(t *testing.T) {
	type capture struct {
		key  string
		body map[string]any
	}
	got := make(chan capture, 1)
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode submit body: %v", err)
			}
			got <- capture{key: r.Header.Get("Idempotency-Key"), body: body}
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-1","status":"running"}`)
			return
		}
		io.WriteString(w, `{"success":true,"id":"job-1","status":"done","token":"tok"}`)
	})

	p := params()
	p.CallbackURL = "https://hooks.example.com/kagedcap"
	p.IdempotencyKey = "order-9182"
	if _, err := kc.Solve(p); err != nil {
		t.Fatalf("Solve: %v", err)
	}

	c := <-got
	if c.key != "order-9182" {
		t.Errorf("Idempotency-Key = %q, want order-9182", c.key)
	}
	if c.body["callback_url"] != "https://hooks.example.com/kagedcap" {
		t.Errorf("callback_url = %v, want the value set on SolveParams", c.body["callback_url"])
	}
	if c.body["task"] != "ReCaptchaV3TaskProxyLess" || c.body["sitekey"] != "6Lc-test" {
		t.Errorf("submit body lost the v1 fields: %v", c.body)
	}
}

func TestSolveGeneratesAnIdempotencyKeyWhenUnsetAndStillOmitsCallbackURL(t *testing.T) {
	// Since 2026-10-20 the SDK retries a submit the gateway may already have accepted, so every
	// submit must carry a key — a fresh UUIDv4 per call when the caller gives none.
	type capture struct {
		key         string
		hasCallback bool
	}
	got := make(chan capture, 2)
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			_, hasCallback := body["callback_url"]
			got <- capture{key: r.Header.Get("Idempotency-Key"), hasCallback: hasCallback}
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-1","status":"running"}`)
			return
		}
		io.WriteString(w, `{"success":true,"id":"job-1","status":"done","token":"tok"}`)
	})

	sub, err := kc.SubmitSolve(params())
	if err != nil {
		t.Fatalf("SubmitSolve: %v", err)
	}
	c := <-got
	if !uuidV4.MatchString(c.key) {
		t.Errorf("Idempotency-Key = %q, want a generated UUIDv4", c.key)
	}
	if sub.IdempotencyKey != c.key {
		t.Errorf("Submission.IdempotencyKey = %q, want the key sent %q", sub.IdempotencyKey, c.key)
	}
	if c.hasCallback {
		t.Error("sent callback_url, want it omitted when unset")
	}

	if _, err := kc.Solve(params()); err != nil {
		t.Fatalf("Solve: %v", err)
	}
	if second := <-got; second.key == c.key {
		t.Errorf("two Solve calls shared Idempotency-Key %q; each call must mint its own or the second returns the first job", c.key)
	}
}

func TestSolveDeprecatedStillPostsTheLegacySynchronousSolve(t *testing.T) {
	type capture struct {
		path string
		body map[string]any
	}
	got := make(chan capture, 1)
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		got <- capture{path: r.URL.Path, body: body}
		io.WriteString(w, `{"success":true,"token":"legacy-tok","task":"ReCaptchaV3EnterpriseTask","score":0.9,"verification":{"hostname":"example.com"}}`)
	})

	p := params()
	p.Enterprise = true
	p.Proxy = "http://user:pass@1.2.3.4:8080"
	p.CallbackURL = "https://hooks.example.com/kagedcap" // async-only; must not reach v1
	res, err := kc.SolveDeprecated(p)
	if err != nil {
		t.Fatalf("SolveDeprecated: %v", err)
	}
	if res.Token != "legacy-tok" {
		t.Errorf("Token = %q, want legacy-tok", res.Token)
	}
	if res.Score == nil || *res.Score != 0.9 {
		t.Errorf("Score = %v, want 0.9 — the legacy path still returns it", res.Score)
	}
	if len(res.Verification) == 0 {
		t.Error("Verification is empty, want the legacy payload")
	}

	c := <-got
	if c.path != "/solve" {
		t.Errorf("path = %q, want /solve", c.path)
	}
	if c.body["task"] != "ReCaptchaV3EnterpriseTask" {
		t.Errorf("task = %v, want ReCaptchaV3EnterpriseTask", c.body["task"])
	}
	if _, ok := c.body["callback_url"]; ok {
		t.Error("callback_url reached the legacy endpoint, which does not accept it")
	}
}

func TestSolveKeepsPollingThroughAnUnknownStatus(t *testing.T) {
	var polls atomic.Int32
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-1","status":"running"}`)
			return
		}
		if polls.Add(1) == 1 {
			// A status the SDK has never heard of must not be read as terminal.
			io.WriteString(w, `{"success":false,"id":"job-1","status":"queued","token":null}`)
			return
		}
		io.WriteString(w, `{"success":true,"id":"job-1","status":"done","token":"tok"}`)
	})

	res, err := kc.Solve(params())
	if err != nil {
		t.Fatalf("Solve: %v", err)
	}
	if res.Token != "tok" {
		t.Errorf("Token = %q, want tok", res.Token)
	}
}

/*
 * reCAPTCHA, tmpt and evaluate ride the async endpoint; Kasada is synchronous.
 *
 * The v2 job row carries the async fleets' full result (a token, or evaluate's decision), so
 * Solve and Evaluate submit to /v2/solve and poll. Kasada stays on the synchronous /solve, which
 * returns its full header set + x_kpsdk_* on the one request — no submit-and-poll. Solve itself
 * rejects a Kasada task, because a KasadaResult has no token for a SolveResult to carry.
 */

func TestSolveUsesTheAsyncEndpointForRecaptcha(t *testing.T) {
	var submitted string
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/solve":
			submitted = r.URL.Path
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-1","status":"running"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v2/solve/job-1":
			io.WriteString(w, `{"success":true,"id":"job-1","status":"done","token":"03AFcWeA"}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	p := params()
	p.Task = "ReCaptchaV3EnterpriseTask"
	if _, err := kc.Solve(p); err != nil {
		t.Fatalf("Solve: %v", err)
	}
	if submitted != "/v2/solve" {
		t.Errorf("reCAPTCHA submitted to %q, want /v2/solve", submitted)
	}
}

func TestSolveRoutesTmptToTheAsyncEndpoint(t *testing.T) {
	var submitted string
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/solve":
			submitted = r.URL.Path
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-t","status":"running"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v2/solve/job-t":
			io.WriteString(w, `{"success":true,"id":"job-t","status":"done","task":"TicketmasterTmptTask","token":"tmpt-cookie"}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	p := params()
	p.Task = "TicketmasterTmptTask"
	res, err := kc.Solve(p)
	if err != nil {
		t.Fatalf("Solve(tmpt): %v", err)
	}
	if submitted != "/v2/solve" {
		t.Errorf("tmpt submitted to %q, want /v2/solve", submitted)
	}
	if res.Token != "tmpt-cookie" {
		t.Errorf("Token = %q, want tmpt-cookie", res.Token)
	}
}

func TestSolveRejectsAKasadaTaskWithoutCallingTheAPI(t *testing.T) {
	// Solve cannot represent a tokenless Kasada result — it must steer to KasadaLogin before
	// spending a request, not poll to "done" and hand back an empty token.
	for _, task := range []string{"KasadaLogin", "KasadaReload"} {
		t.Run(task, func(t *testing.T) {
			kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("Solve(%s) hit the API at %s; it should reject before any request", task, r.URL.Path)
			})
			p := params()
			p.Task = task
			_, err := kc.Solve(p)
			if got := kcErr(t, err).Code; got != "validation_error" {
				t.Errorf("Code = %q, want validation_error", got)
			}
		})
	}
}

func kasadaParams() kagedcap.KasadaParams {
	return kagedcap.KasadaParams{Site: "ticketmaster", Proxy: "http://u:p@1.2.3.4:8080"}
}

func TestKasadaLoginPostsTheSynchronousSolveAndReturnsTheHeaderSet(t *testing.T) {
	var calls atomic.Int32
	var hit, method, task string
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		hit, method = r.URL.Path, r.Method
		task, _ = body["task"].(string)
		io.WriteString(w, `{"success":true,"task":"KasadaLogin","site":"ticketmaster","headers":{"user-agent":"UA"},"x_kpsdk_ct":"ct","x_kpsdk_cd":"cd","x_kpsdk_v":"v","x_kpsdk_h":"h","kpsdk_st":123,"hash":"hh","reload":true,"user_agent":"UA"}`)
	})
	res, err := kc.KasadaLogin(kasadaParams())
	if err != nil {
		t.Fatalf("KasadaLogin: %v", err)
	}
	if method != http.MethodPost || hit != "/solve" || task != "KasadaLogin" {
		t.Errorf("request was %s %q task=%q, want POST /solve KasadaLogin", method, hit, task)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("made %d requests, want 1 — /solve returns the result directly, no poll", n)
	}
	if res.XKpsdkCt != "ct" || res.Headers["user-agent"] != "UA" || res.KpsdkST == nil || *res.KpsdkST != 123 {
		t.Errorf("result lost fields: %+v", res)
	}
}

func TestKasadaReloadResendsThePriorSessionToTheSynchronousSolve(t *testing.T) {
	st := int64(123)
	prev := &kagedcap.KasadaResult{KpsdkST: &st, Hash: "hh", Site: "ticketmaster", XKpsdkCt: "ct0"}
	var calls atomic.Int32
	var hit string
	var sent map[string]any
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		hit = r.URL.Path
		json.NewDecoder(r.Body).Decode(&sent)
		io.WriteString(w, `{"success":true,"task":"KasadaReload","x_kpsdk_ct":"ct1","x_kpsdk_cd":"cd1"}`)
	})
	res, err := kc.KasadaReload(prev)
	if err != nil {
		t.Fatalf("KasadaReload: %v", err)
	}
	if hit != "/solve" {
		t.Errorf("hit %q, want /solve", hit)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("made %d requests, want 1 — the sync endpoint returns the result directly, no poll", n)
	}
	if sent["task"] != "KasadaReload" || sent["kpsdk_st"].(float64) != 123 || sent["hash"] != "hh" {
		t.Errorf("reload body lost the prior session: %v", sent)
	}
	if res.XKpsdkCt != "ct1" {
		t.Errorf("XKpsdkCt = %q, want the refreshed ct1", res.XKpsdkCt)
	}
}

func TestKasadaReloadRequiresAPriorSession(t *testing.T) {
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("KasadaReload(nil) hit the API; it must validate first")
	})
	_, err := kc.KasadaReload(nil)
	if got := kcErr(t, err).Code; got != "validation_error" {
		t.Errorf("Code = %q, want validation_error", got)
	}
}

func evalParams() kagedcap.EvaluateParams {
	return kagedcap.EvaluateParams{URL: "https://auth.ticketmaster.com/x", Proxy: "http://u:p@1.2.3.4:8080", PollInterval: 5 * time.Millisecond, Deadline: 2 * time.Second}
}

func TestEvaluatePollsTheAsyncEndpointAndReturnsTheDecision(t *testing.T) {
	var submitted string
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/solve":
			submitted = r.URL.Path
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-e","status":"running"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v2/solve/job-e":
			io.WriteString(w, `{"success":true,"id":"job-e","status":"done","task":"EvaluateTask","token":"ev-tok","decision":"allow"}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	})
	res, err := kc.Evaluate(evalParams())
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if submitted != "/v2/solve" {
		t.Errorf("evaluate submitted to %q, want /v2/solve", submitted)
	}
	if res.Token != "ev-tok" || res.Decision != "allow" {
		t.Errorf("result = %+v, want token ev-tok decision allow", res)
	}
}

func TestEvaluateKeepsAChallengeDecisionEvenWithNoToken(t *testing.T) {
	// A challenge verdict legitimately has an empty token, so evaluate must key "expired" on a
	// missing decision, not a missing token — otherwise every challenge reads as expired.
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-e","status":"running"}`)
			return
		}
		io.WriteString(w, `{"success":true,"id":"job-e","status":"done","task":"EvaluateTask","token":"","decision":"challenge"}`)
	})
	res, err := kc.Evaluate(evalParams())
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if res.Decision != "challenge" {
		t.Errorf("Decision = %q, want challenge", res.Decision)
	}
}

func TestEvaluateReportsResultExpiredWhenDoneCarriesNoDecision(t *testing.T) {
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-e","status":"running"}`)
			return
		}
		io.WriteString(w, `{"success":true,"id":"job-e","status":"done"}`)
	})
	_, err := kc.Evaluate(evalParams())
	if got := kcErr(t, err).Code; got != "result_expired" {
		t.Errorf("Code = %q, want result_expired", got)
	}
}

func TestDeprecatedMethodsStillUseTheSynchronousEndpoint(t *testing.T) {
	st := int64(1)
	cases := []struct {
		name string
		call func(*kagedcap.Client) error
	}{
		{"KasadaLoginDeprecated", func(c *kagedcap.Client) error { _, e := c.KasadaLoginDeprecated(kasadaParams()); return e }},
		{"KasadaReloadDeprecated", func(c *kagedcap.Client) error {
			_, e := c.KasadaReloadDeprecated(&kagedcap.KasadaResult{KpsdkST: &st, Hash: "h"})
			return e
		}},
		{"EvaluateDeprecated", func(c *kagedcap.Client) error { _, e := c.EvaluateDeprecated(evalParams()); return e }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hit string
			kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
				hit = r.URL.Path
				io.WriteString(w, `{"success":true,"task":"t","x_kpsdk_ct":"ct","decision":"allow"}`)
			})
			if err := tc.call(kc); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if hit != "/solve" {
				t.Errorf("%s hit %q, want the legacy /solve", tc.name, hit)
			}
		})
	}
}

func TestSubmitSolveAndGetSolveDriveThePrimitivesDirectly(t *testing.T) {
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v2/solve":
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-p","status":"running"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v2/solve/job-p":
			io.WriteString(w, `{"success":true,"id":"job-p","status":"done","token":"tok-p"}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	})
	sub, err := kc.SubmitSolve(params())
	if err != nil {
		t.Fatalf("SubmitSolve: %v", err)
	}
	if sub.ID != "job-p" || sub.Status != "running" {
		t.Errorf("Submission = %+v, want id job-p status running", sub)
	}
	job, err := kc.GetSolve(sub.ID)
	if err != nil {
		t.Fatalf("GetSolve: %v", err)
	}
	if job.Status != "done" || job.Token != "tok-p" {
		t.Errorf("Job = %+v, want status done token tok-p", job)
	}
}

/*
 * Retries (2026-10-20).
 *
 * The Railway edge in front of the gateway sheds submits and polls with a bare 429 before they
 * reach the API, and the API itself answers 429/5xx under load. Those requests succeed a moment
 * later, so the SDK retries them — with the same Idempotency-Key, so a submit the gateway did
 * accept is never solved twice — until the deadline. Verdicts on the request (4xx, a failed
 * job) still come straight back.
 */

// retryGateway is a stand-in gateway whose submit and poll answer from scripted queues, then
// settle on 202 / done. It records every submit's Idempotency-Key and arrival time.
type retryGateway struct {
	mu          sync.Mutex
	submitQueue []func(http.ResponseWriter)
	pollQueue   []func(http.ResponseWriter)
	submits     int
	polls       int
	keys        []string
	submitAt    []time.Time
}

func status(code int, body string) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		w.WriteHeader(code)
		io.WriteString(w, body)
	}
}

// edge429 is what the Railway edge sends: no JSON envelope at all.
var edge429 = func(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusTooManyRequests)
	io.WriteString(w, "rate limited")
}

func (g *retryGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v2/solve":
		g.submits++
		g.keys = append(g.keys, r.Header.Get("Idempotency-Key"))
		g.submitAt = append(g.submitAt, time.Now())
		if len(g.submitQueue) > 0 {
			next := g.submitQueue[0]
			g.submitQueue = g.submitQueue[1:]
			next(w)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		io.WriteString(w, `{"success":true,"id":"job-r","status":"running"}`)
	case r.Method == http.MethodGet && r.URL.Path == "/v2/solve/job-r":
		g.polls++
		if len(g.pollQueue) > 0 {
			next := g.pollQueue[0]
			g.pollQueue = g.pollQueue[1:]
			next(w)
			return
		}
		io.WriteString(w, `{"success":true,"id":"job-r","status":"done","token":"tok-r"}`)
	default:
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"success":false,"error":"not_found","message":"no route"}`)
	}
}

func (g *retryGateway) counts() (submits, polls int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.submits, g.polls
}

func TestSolveRetriesTheSubmitThroughRateLimitsWithOneIdempotencyKey(t *testing.T) {
	g := &retryGateway{submitQueue: []func(http.ResponseWriter){
		edge429,
		status(http.StatusTooManyRequests, `{"success":false,"error":"overloaded","message":"shedding load","request_id":"req-1"}`),
	}}
	kc := newClient(t, g.ServeHTTP, fastRetries())

	res, err := kc.Solve(params())
	if err != nil {
		t.Fatalf("Solve: %v", err)
	}
	if res.Token != "tok-r" {
		t.Errorf("Token = %q, want tok-r", res.Token)
	}
	if submits, _ := g.counts(); submits != 3 {
		t.Errorf("submitted %d times, want 3 (429, 429, 202)", submits)
	}
	if !uuidV4.MatchString(g.keys[0]) {
		t.Errorf("Idempotency-Key = %q, want a generated UUIDv4", g.keys[0])
	}
	for i, k := range g.keys[1:] {
		if k != g.keys[0] {
			t.Errorf("retry %d sent Idempotency-Key %q, want the same %q — a new key would be a second job", i+1, k, g.keys[0])
		}
	}
}

func TestSolveRetriesTheSubmitByDefault(t *testing.T) {
	// No retry option at all: New must install the policy, not leave it off.
	g := &retryGateway{submitQueue: []func(http.ResponseWriter){edge429}}
	kc := newClient(t, g.ServeHTTP)

	if _, err := kc.Solve(params()); err != nil {
		t.Fatalf("Solve with the default policy: %v", err)
	}
	if submits, _ := g.counts(); submits != 2 {
		t.Errorf("submitted %d times, want 2", submits)
	}
}

func TestSolveRetriesThePollThroughGatewayErrors(t *testing.T) {
	g := &retryGateway{pollQueue: []func(http.ResponseWriter){
		status(http.StatusServiceUnavailable, `{"success":false,"error":"no_capacity","message":"no solver capacity","request_id":"req-2"}`),
		status(http.StatusBadGateway, `<html>bad gateway</html>`),
	}}
	kc := newClient(t, g.ServeHTTP, fastRetries())

	res, err := kc.Solve(params())
	if err != nil {
		t.Fatalf("Solve: %v", err)
	}
	if res.Token != "tok-r" {
		t.Errorf("Token = %q, want tok-r", res.Token)
	}
	if _, polls := g.counts(); polls != 3 {
		t.Errorf("polled %d times, want 3 (503, 502, 200)", polls)
	}
}

func TestSolveDoesNotRetryA400(t *testing.T) {
	g := &retryGateway{submitQueue: []func(http.ResponseWriter){
		status(http.StatusBadRequest, `{"success":false,"error":"validation_error","message":"sitekey is required","request_id":"req-3"}`),
	}}
	kc := newClient(t, g.ServeHTTP, fastRetries())

	_, err := kc.Solve(params())
	e := kcErr(t, err)
	if e.Code != "validation_error" || e.Status != http.StatusBadRequest {
		t.Errorf("got code=%q status=%d, want validation_error/400", e.Code, e.Status)
	}
	if e.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", e.Attempts)
	}
	if submits, polls := g.counts(); submits != 1 || polls != 0 {
		t.Errorf("submits=%d polls=%d after a 400, want 1/0 — a verdict on the request is never retried", submits, polls)
	}
}

func TestSolveDoesNotRetryTerminalCodesOnARetryableStatus(t *testing.T) {
	// The status says "try again" but the code says the request — or the key — is wrong and
	// will not come right within any deadline.
	cases := []struct {
		code   string
		status int
	}{
		{"unsupported_task", http.StatusServiceUnavailable},
		{"key_frozen", http.StatusTooManyRequests},
		{"subscription_quota_exhausted", http.StatusServiceUnavailable},
		{"subscription_past_due", http.StatusServiceUnavailable},
		{"host_not_allowed", http.StatusServiceUnavailable},
		{"idempotency_conflict", http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			g := &retryGateway{submitQueue: []func(http.ResponseWriter){
				status(tc.status, fmt.Sprintf(`{"success":false,"error":%q,"message":"no"}`, tc.code)),
			}}
			kc := newClient(t, g.ServeHTTP, fastRetries())

			_, err := kc.Solve(params())
			e := kcErr(t, err)
			if e.Code != tc.code || e.Attempts != 1 {
				t.Errorf("got code=%q attempts=%d, want %s after 1", e.Code, e.Attempts, tc.code)
			}
			if submits, _ := g.counts(); submits != 1 {
				t.Errorf("submitted %d times, want 1", submits)
			}
		})
	}
}

func TestSolveGivesUpAtTheDeadlineUnderAConstant429(t *testing.T) {
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) { edge429(w) }, fastRetries())

	p := params()
	p.Deadline = time.Second
	start := time.Now()
	_, err := kc.Solve(p)
	elapsed := time.Since(start)

	e := kcErr(t, err)
	// A retry is not started with under 500ms of budget left, so the run ends well before the
	// deadline — with the budget handed back to the caller, not burned on a try that cannot answer.
	if elapsed > p.Deadline-200*time.Millisecond {
		t.Errorf("gave up after %s; want the last 500ms of the %s deadline left unspent", elapsed, p.Deadline)
	}
	if elapsed < 2*kagedcap.MinRetryDelay {
		t.Errorf("gave up after %s, want at least a couple of 50ms waits before the deadline bit", elapsed)
	}
	if e.Status != http.StatusTooManyRequests || e.Code != "error" {
		t.Errorf("got status=%d code=%q, want the last response's bare 429/error, not solve_timeout", e.Status, e.Code)
	}
	if e.Attempts < 2 {
		t.Errorf("Attempts = %d, want more than one try before giving up", e.Attempts)
	}
	for _, want := range []string{"429", fmt.Sprintf("%d attempts", e.Attempts), "gave up"} {
		if !strings.Contains(e.Error(), want) {
			t.Errorf("error %q does not mention %q", e.Error(), want)
		}
	}
	if e.IdempotencyKey == "" {
		t.Error("IdempotencyKey is empty on the submit error; the caller needs it to find the job")
	}
}

func TestSolveReportsTheLastRealResponseWhenTheDeadlineCutsARetryShort(t *testing.T) {
	// First submit: a bare edge 429. Second: the edge holds the connection until the deadline
	// kills it. The error must be the 429 — the only real answer — never the cut-short request.
	var submits atomic.Int32
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if submits.Add(1) == 1 {
			edge429(w)
			return
		}
		// net/http only notices the client going away once the body is drained; the timer is a
		// backstop so a regression here cannot hang srv.Close.
		io.ReadAll(r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}, fastRetries())

	p := params()
	p.Deadline = 700 * time.Millisecond
	_, err := kc.Solve(p)
	e := kcErr(t, err)
	if e.Status != http.StatusTooManyRequests || e.Code != "error" {
		t.Errorf("got status=%d code=%q message=%q, want the 429 that answered, not the request the deadline cut", e.Status, e.Code, e.Message)
	}
	if e.Attempts != 2 || submits.Load() != 2 {
		t.Errorf("attempts=%d submits=%d, want 2/2", e.Attempts, submits.Load())
	}
	if !strings.Contains(e.Message, "HTTP 429") {
		t.Errorf("message %q does not name the last real response", e.Message)
	}
}

func TestRetryErrorsReadWellAfterASingleAttempt(t *testing.T) {
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		status(http.StatusServiceUnavailable, `{"success":false,"error":"no_capacity","message":"no solver capacity"}`)(w)
	}, kagedcap.WithRetryPolicy(kagedcap.RetryPolicy{MaxAttempts: 1}))

	_, err := kc.Solve(params())
	e := kcErr(t, err)
	if e.Attempts != 1 || e.Code != "no_capacity" {
		t.Errorf("got attempts=%d code=%q, want 1/no_capacity", e.Attempts, e.Code)
	}
	if !strings.Contains(e.Message, "after 1 attempt ") || strings.Contains(e.Message, "1 attempts") {
		t.Errorf("message %q, want a properly singular \"1 attempt\"", e.Message)
	}
}

func TestSolveReportsCanceledWhenTheContextIsCanceledDuringABackoff(t *testing.T) {
	// A 5s Retry-After parks the run in its backoff; the caller cancels 100ms in. The answer is
	// canceled — the caller's own doing — not the 429 the run was waiting out.
	var submits atomic.Int32
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		submits.Add(1)
		w.Header().Set("Retry-After", "5")
		status(http.StatusTooManyRequests, `{"success":false,"error":"rate_limited","message":"slow down","request_id":"req-9"}`)(w)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	p := params()
	p.Deadline = 10 * time.Second
	start := time.Now()
	_, err := kc.SolveContext(ctx, p)
	elapsed := time.Since(start)

	e := kcErr(t, err)
	if e.Code != "canceled" {
		t.Errorf("Code = %q, want canceled", e.Code)
	}
	if e.Attempts != 1 || submits.Load() != 1 {
		t.Errorf("attempts=%d submits=%d, want 1/1", e.Attempts, submits.Load())
	}
	if !strings.Contains(e.Message, "rate_limited") {
		t.Errorf("message %q should still mention the last response", e.Message)
	}
	if elapsed > 3*time.Second {
		t.Errorf("returned after %s; the cancel should have ended the 5s backoff at once", elapsed)
	}
}

func TestSolveHonoursRetryAfterOnTheSubmit(t *testing.T) {
	cases := []struct {
		name   string
		header func() string
		min    time.Duration
	}{
		{"delay-seconds", func() string { return "2" }, 1900 * time.Millisecond},
		// An HTTP-date has whole-second resolution, so "now + 1.5s" lands 0.5-1.5s out.
		{"http-date", func() string { return time.Now().Add(1500 * time.Millisecond).UTC().Format(http.TimeFormat) }, 400 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := &retryGateway{submitQueue: []func(http.ResponseWriter){func(w http.ResponseWriter) {
				w.Header().Set("Retry-After", tc.header())
				w.WriteHeader(http.StatusTooManyRequests)
				io.WriteString(w, `{"success":false,"error":"rate_limited","message":"slow down"}`)
			}}}
			kc := newClient(t, g.ServeHTTP, fastRetries())

			p := params()
			p.Deadline = 10 * time.Second
			if _, err := kc.Solve(p); err != nil {
				t.Fatalf("Solve: %v", err)
			}
			if submits, _ := g.counts(); submits != 2 {
				t.Fatalf("submitted %d times, want 2", submits)
			}
			gap := g.submitAt[1].Sub(g.submitAt[0])
			if gap < tc.min || gap > tc.min+2*time.Second {
				t.Errorf("retried after %s, want Retry-After honoured (about %s, not the %s backoff)", gap, tc.min, 4*time.Millisecond)
			}
		})
	}
}

func TestRetryDelayStaysUnderTheCapAndIsJittered(t *testing.T) {
	p := kagedcap.DefaultRetryPolicy()
	floor := kagedcap.MinRetryDelay
	for failures := 1; failures <= 12; failures++ {
		ceiling := kagedcap.DefaultRetryInitialBackoff << (failures - 1)
		if ceiling > kagedcap.DefaultRetryMaxBackoff {
			ceiling = kagedcap.DefaultRetryMaxBackoff
		}
		seen := map[time.Duration]bool{}
		for i := 0; i < 200; i++ {
			d := p.Delay(failures, 0)
			if d < floor || d > ceiling {
				t.Fatalf("Delay(%d) = %s, want within [%s, %s]", failures, d, floor, ceiling)
			}
			seen[d] = true
		}
		if len(seen) < 2 {
			t.Errorf("Delay(%d) returned the same value 200 times; want full jitter", failures)
		}
	}
	if d := p.Delay(1, 30*time.Second); d != kagedcap.DefaultRetryMaxRetryAfter {
		t.Errorf("Delay with Retry-After 30s = %s, want capped at %s", d, kagedcap.DefaultRetryMaxRetryAfter)
	}
	if d := p.Delay(1, 3*time.Second); d != 3*time.Second {
		t.Errorf("Delay with Retry-After 3s = %s, want exactly 3s", d)
	}
	if d := p.Delay(1, time.Millisecond); d != floor {
		t.Errorf("Delay with Retry-After 1ms = %s, want raised to the %s floor", d, floor)
	}
	if d := p.Delay(50, 0); d > kagedcap.DefaultRetryMaxBackoff {
		t.Errorf("Delay(50) = %s, want no overflow past the cap", d)
	}
}

func TestRetryDelayNeverGoesUnderTheFloor(t *testing.T) {
	floor := kagedcap.MinRetryDelay
	tiny := kagedcap.RetryPolicy{InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond}
	for failures := 1; failures <= 5; failures++ {
		if d := tiny.Delay(failures, 0); d != floor {
			t.Errorf("tiny policy Delay(%d) = %s, want exactly the %s floor", failures, d, floor)
		}
	}
	// A zero or negative InitialBackoff is not "no wait": it means the default curve.
	for _, p := range []kagedcap.RetryPolicy{{InitialBackoff: 0}, {InitialBackoff: -time.Second}} {
		for i := 0; i < 100; i++ {
			if d := p.Delay(1, 0); d < floor || d > kagedcap.DefaultRetryInitialBackoff {
				t.Fatalf("policy %+v Delay(1) = %s, want within [%s, %s]", p, d, floor, kagedcap.DefaultRetryInitialBackoff)
			}
		}
	}
}

func TestRetryAfterIsClampedBeforeItBecomesADuration(t *testing.T) {
	// The exposed Error.RetryAfter is never negative and never past 24h, whatever the header
	// says; the wait it drives is capped by MaxRetryAfter and floored at MinRetryDelay.
	cases := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{"absent", "", 0},
		{"zero", "0", 0},
		{"negative", "-30", 0},
		{"garbage", "soon", 0},
		{"float", "1.5", 0},
		{"two seconds", "2", 2 * time.Second},
		{"overflowing seconds", "10000000000", 24 * time.Hour},
		{"past int64", "99999999999999999999999", 24 * time.Hour},
		{"negative past int64", "-99999999999999999999999", 0},
		{"date before 1970", "Thu, 01 Jan 1960 00:00:00 GMT", 0},
		{"date in the past", "Wed, 21 Oct 2015 07:28:00 GMT", 0},
		{"date in 9999", "Fri, 31 Dec 9999 23:59:59 GMT", 24 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var submitAt []time.Time
			var mu sync.Mutex
			kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				submitAt = append(submitAt, time.Now())
				mu.Unlock()
				if tc.header != "" {
					w.Header().Set("Retry-After", tc.header)
				}
				edge429(w)
			}, kagedcap.WithRetryPolicy(kagedcap.RetryPolicy{
				MaxAttempts: 2, InitialBackoff: kagedcap.MinRetryDelay, MaxBackoff: kagedcap.MinRetryDelay, MaxRetryAfter: 80 * time.Millisecond,
			}))

			_, err := kc.Solve(params())
			e := kcErr(t, err)
			if e.RetryAfter != tc.want {
				t.Errorf("RetryAfter = %s, want %s", e.RetryAfter, tc.want)
			}
			if e.RetryAfter < 0 {
				t.Errorf("RetryAfter = %s is negative", e.RetryAfter)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(submitAt) != 2 {
				t.Fatalf("submitted %d times, want 2", len(submitAt))
			}
			gap := submitAt[1].Sub(submitAt[0])
			if gap < kagedcap.MinRetryDelay {
				t.Errorf("retried after %s, under the %s floor", gap, kagedcap.MinRetryDelay)
			}
			if gap > time.Second {
				t.Errorf("retried after %s; the wait must be capped by MaxRetryAfter, not the header", gap)
			}
		})
	}
}

func TestWithoutRetriesReturnsTheFirst429(t *testing.T) {
	g := &retryGateway{submitQueue: []func(http.ResponseWriter){
		status(http.StatusTooManyRequests, `{"success":false,"error":"rate_limited","message":"slow down","request_id":"req-4"}`),
	}}
	kc := newClient(t, g.ServeHTTP, kagedcap.WithoutRetries())

	_, err := kc.Solve(params())
	e := kcErr(t, err)
	if e.Code != "rate_limited" || e.Status != http.StatusTooManyRequests || e.RequestID != "req-4" {
		t.Errorf("got %+v, want the 429 envelope untouched", e)
	}
	if submits, _ := g.counts(); submits != 1 {
		t.Errorf("submitted %d times with retries off, want 1", submits)
	}
}

func TestMaxAttemptsBoundsTheRetries(t *testing.T) {
	var submits atomic.Int32
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		submits.Add(1)
		status(http.StatusServiceUnavailable, `{"success":false,"error":"solver_unavailable","message":"node unreachable"}`)(w)
	}, kagedcap.WithRetryPolicy(kagedcap.RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}))

	_, err := kc.Solve(params())
	e := kcErr(t, err)
	if e.Code != "solver_unavailable" || e.Attempts != 3 {
		t.Errorf("got code=%q attempts=%d, want solver_unavailable after 3", e.Code, e.Attempts)
	}
	if got := submits.Load(); got != 3 {
		t.Errorf("submitted %d times, want exactly MaxAttempts=3", got)
	}
}

func TestSubmitRetriesAfterATransportFailure(t *testing.T) {
	var submits atomic.Int32
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if submits.Add(1) == 1 {
			// Drop the connection mid-request: the client sees EOF / reset, not an HTTP status.
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Fatalf("hijack: %v", err)
			}
			conn.Close()
			return
		}
		w.WriteHeader(http.StatusAccepted)
		io.WriteString(w, `{"success":true,"id":"job-n","status":"running"}`)
	}, fastRetries())

	sub, err := kc.SubmitSolve(params())
	if err != nil {
		t.Fatalf("SubmitSolve: %v", err)
	}
	if sub.ID != "job-n" || submits.Load() != 2 {
		t.Errorf("got id=%q after %d submits, want job-n after 2", sub.ID, submits.Load())
	}
}

/*
 * The legacy /solve retries only what never reached the gateway.
 *
 * The gateway claims the Idempotency-Key (SET NX EX 120) as its very first step, so once it has
 * answered — even with a 429/503 envelope — a resend of the same key can only come back 409
 * idempotency_conflict. The only honest outcome of such a failure is the failure itself. What
 * the edge sheds (a bare, non-JSON 429/502/503/504) and what never connected at all can be
 * resent safely, and only under a key, since a keyless resend of a request that did arrive
 * would be a second, billable solve — which is why every SolveDeprecated/EvaluateDeprecated
 * call carries one, minted per call when the caller gives none, and why no retry starts once
 * the gateway's 120s claim could have lapsed. Kasada has no key field and goes out once.
 */

// edgePage is what the Railway/Cloudflare edge sends when the app is unreachable or shedding:
// a status with an HTML or plain-text body, never our JSON envelope.
func edgePage(code int) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(code)
		io.WriteString(w, "<html>upstream unavailable</html>")
	}
}

// keyedSolveGateway is a stand-in for the legacy /solve: it answers from a scripted queue, then
// settles on a token, and records every request's Idempotency-Key.
type keyedSolveGateway struct {
	mu    sync.Mutex
	queue []func(http.ResponseWriter)
	keys  []string
}

func (g *keyedSolveGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.keys = append(g.keys, r.Header.Get("Idempotency-Key"))
	if len(g.queue) > 0 {
		next := g.queue[0]
		g.queue = g.queue[1:]
		next(w)
		return
	}
	io.WriteString(w, `{"success":true,"token":"legacy-tok","task":"ReCaptchaV3TaskProxyLess","decision":"allow"}`)
}

func (g *keyedSolveGateway) seen() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.keys...)
}

func dropConnection(w http.ResponseWriter) {
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		panic("hijack: " + err.Error())
	}
	conn.Close()
}

func TestSolveDeprecatedRetriesUnderAnIdempotencyKey(t *testing.T) {
	t.Run("a bare call mints one key and retries an edge 503 under it", func(t *testing.T) {
		g := &keyedSolveGateway{queue: []func(http.ResponseWriter){edgePage(http.StatusServiceUnavailable)}}
		kc := newClient(t, g.ServeHTTP, fastRetries())

		res, err := kc.SolveDeprecated(params())
		if err != nil {
			t.Fatalf("SolveDeprecated: %v", err)
		}
		keys := g.seen()
		if res.Token != "legacy-tok" || len(keys) != 2 {
			t.Fatalf("token=%q keys=%v, want legacy-tok after 2 requests (503, 200)", res.Token, keys)
		}
		if !uuidV4.MatchString(keys[0]) {
			t.Errorf("Idempotency-Key = %q, want a generated UUIDv4 — a bare /solve is no longer single-shot", keys[0])
		}
		if keys[1] != keys[0] {
			t.Errorf("retry sent Idempotency-Key %q, want the same %q — a new key would be a second solve", keys[1], keys[0])
		}

		if _, err := kc.SolveDeprecated(params()); err != nil {
			t.Fatalf("second SolveDeprecated: %v", err)
		}
		if again := g.seen(); again[2] == keys[0] {
			t.Errorf("two bare calls shared Idempotency-Key %q; each call must mint its own", keys[0])
		}
	})

	t.Run("a bare call still takes the gateway's own 429 envelope as final", func(t *testing.T) {
		g := &keyedSolveGateway{queue: []func(http.ResponseWriter){
			status(http.StatusTooManyRequests, `{"success":false,"error":"overloaded","message":"shedding load","request_id":"req-b"}`),
		}}
		kc := newClient(t, g.ServeHTTP, fastRetries())

		_, err := kc.SolveDeprecated(params())
		e := kcErr(t, err)
		if e.Code != "overloaded" || e.Status != http.StatusTooManyRequests || e.Attempts != 1 || e.RequestID != "req-b" {
			t.Errorf("got %+v, want the gateway's overloaded 429 untouched after 1 attempt", e)
		}
		keys := g.seen()
		if len(keys) != 1 {
			t.Fatalf("made %d requests, want 1 — the gateway holds the key, a resend could only answer 409", len(keys))
		}
		if !uuidV4.MatchString(keys[0]) || e.IdempotencyKey != keys[0] {
			t.Errorf("Idempotency-Key sent=%q on error=%q, want one generated UUIDv4 on both", keys[0], e.IdempotencyKey)
		}
	})

	t.Run("with a key an edge 503 is retried under the same key", func(t *testing.T) {
		g := &keyedSolveGateway{queue: []func(http.ResponseWriter){edgePage(http.StatusServiceUnavailable), edge429}}
		kc := newClient(t, g.ServeHTTP, fastRetries())

		p := params()
		p.IdempotencyKey = "order-77"
		res, err := kc.SolveDeprecated(p)
		if err != nil {
			t.Fatalf("SolveDeprecated: %v", err)
		}
		if keys := g.seen(); res.Token != "legacy-tok" || len(keys) != 3 || keys[0] != "order-77" || keys[1] != "order-77" || keys[2] != "order-77" {
			t.Errorf("token=%q keys=%v, want legacy-tok after 3 requests all carrying order-77", res.Token, keys)
		}
	})

	t.Run("with a key a dropped connection is retried under the same key", func(t *testing.T) {
		g := &keyedSolveGateway{queue: []func(http.ResponseWriter){dropConnection}}
		kc := newClient(t, g.ServeHTTP, fastRetries())

		p := params()
		p.IdempotencyKey = "order-79"
		res, err := kc.SolveDeprecated(p)
		if err != nil {
			t.Fatalf("SolveDeprecated: %v", err)
		}
		if keys := g.seen(); res.Token != "legacy-tok" || len(keys) != 2 || keys[1] != "order-79" {
			t.Errorf("token=%q keys=%v, want legacy-tok after 2 requests under order-79", res.Token, keys)
		}
	})

	t.Run("with a key the gateway's own 429 and 503 envelopes are final", func(t *testing.T) {
		envelopes := []struct {
			code   string
			status int
		}{
			{"no_capacity", http.StatusServiceUnavailable},
			{"overloaded", http.StatusTooManyRequests},
			{"internal_error", http.StatusInternalServerError},
			{"solve_timeout", http.StatusGatewayTimeout},
		}
		for _, tc := range envelopes {
			t.Run(tc.code, func(t *testing.T) {
				g := &keyedSolveGateway{queue: []func(http.ResponseWriter){
					status(tc.status, fmt.Sprintf(`{"success":false,"error":%q,"message":"from the gateway","request_id":"req-s"}`, tc.code)),
				}}
				kc := newClient(t, g.ServeHTTP, fastRetries())

				p := params()
				p.IdempotencyKey = "order-80"
				_, err := kc.SolveDeprecated(p)
				e := kcErr(t, err)
				if e.Code != tc.code || e.Attempts != 1 || e.RequestID != "req-s" {
					t.Errorf("got %+v, want the gateway's %s untouched after 1 attempt", e, tc.code)
				}
				if keys := g.seen(); len(keys) != 1 {
					t.Errorf("made %d requests, want 1 — the gateway already holds this key, a resend can only answer 409", len(keys))
				}
			})
		}
	})

	t.Run("a 409 on the retry reports the failure before it", func(t *testing.T) {
		g := &keyedSolveGateway{queue: []func(http.ResponseWriter){
			edgePage(http.StatusBadGateway),
			status(http.StatusConflict, `{"success":false,"error":"idempotency_conflict","message":"key in use","request_id":"req-c"}`),
		}}
		kc := newClient(t, g.ServeHTTP, fastRetries())

		p := params()
		p.IdempotencyKey = "order-81"
		_, err := kc.SolveDeprecated(p)
		e := kcErr(t, err)
		if e.Status != http.StatusBadGateway || e.Code != "error" || e.Attempts != 2 {
			t.Errorf("got status=%d code=%q attempts=%d, want the edge 502 after 2 — the 409 only proves the first request arrived", e.Status, e.Code, e.Attempts)
		}
		if !strings.Contains(e.Message, "idempotency_conflict") || !strings.Contains(e.Message, "HTTP 502") {
			t.Errorf("message %q should explain the conflict and name the last response", e.Message)
		}
		if e.IdempotencyKey != "order-81" {
			t.Errorf("IdempotencyKey = %q, want order-81", e.IdempotencyKey)
		}
	})

	t.Run("a 409 on the first attempt is the caller's own conflict", func(t *testing.T) {
		g := &keyedSolveGateway{queue: []func(http.ResponseWriter){
			status(http.StatusConflict, `{"success":false,"error":"idempotency_conflict","message":"key reused with a different body"}`),
		}}
		kc := newClient(t, g.ServeHTTP, fastRetries())

		p := params()
		p.IdempotencyKey = "order-82"
		_, err := kc.SolveDeprecated(p)
		e := kcErr(t, err)
		if e.Code != "idempotency_conflict" || e.Status != http.StatusConflict || e.Attempts != 1 {
			t.Errorf("got %+v, want the 409 itself after 1 attempt", e)
		}
	})

	t.Run("solve_failed is a verdict on the synchronous endpoint", func(t *testing.T) {
		g := &keyedSolveGateway{queue: []func(http.ResponseWriter){
			status(http.StatusBadGateway, `{"success":false,"error":"solve_failed","message":"the solve ran and failed"}`),
		}}
		kc := newClient(t, g.ServeHTTP, fastRetries())

		p := params()
		p.IdempotencyKey = "order-78"
		_, err := kc.SolveDeprecated(p)
		if got := kcErr(t, err).Code; got != "solve_failed" {
			t.Errorf("Code = %q, want solve_failed", got)
		}
		if keys := g.seen(); len(keys) != 1 {
			t.Errorf("made %d requests, want 1 — a solve that ran and failed is not retried on /solve", len(keys))
		}
	})
}

func TestEvaluateDeprecatedHonoursTheIdempotencyKeyLikeSolveDeprecated(t *testing.T) {
	g := &keyedSolveGateway{queue: []func(http.ResponseWriter){edgePage(http.StatusServiceUnavailable)}}
	kc := newClient(t, g.ServeHTTP, fastRetries())

	p := evalParams()
	p.IdempotencyKey = "eval-5"
	res, err := kc.EvaluateDeprecated(p)
	if err != nil {
		t.Fatalf("EvaluateDeprecated: %v", err)
	}
	if keys := g.seen(); res.Decision != "allow" || len(keys) != 2 || keys[0] != "eval-5" || keys[1] != "eval-5" {
		t.Errorf("decision=%q keys=%v, want allow after 2 requests both carrying eval-5", res.Decision, keys)
	}

	g = &keyedSolveGateway{queue: []func(http.ResponseWriter){edgePage(http.StatusServiceUnavailable)}}
	kc = newClient(t, g.ServeHTTP, fastRetries())
	if _, err := kc.EvaluateDeprecated(evalParams()); err != nil {
		t.Fatalf("bare EvaluateDeprecated: %v", err)
	}
	if keys := g.seen(); len(keys) != 2 || !uuidV4.MatchString(keys[0]) || keys[1] != keys[0] {
		t.Errorf("keys = %v, want 2 requests under one generated UUIDv4", keys)
	}
}

func TestSolveDeprecatedStopsRetryingOnceTheKeyClaimCouldHaveLapsed(t *testing.T) {
	// The gateway's claim on the key lives 120s. A retry past that would not be deduplicated but
	// run — and billed — as a fresh solve, so none starts more than legacyRetryWindow (100s) after
	// the first attempt. The window is shrunk here so the test provokes the guard without sleeping;
	// fastRetries pins every wait to exactly MinRetryDelay (50ms).
	t.Cleanup(kagedcap.SetLegacyRetryWindow(120 * time.Millisecond))

	t.Run("gives up with the last real response, attempts and elapsed", func(t *testing.T) {
		var requests atomic.Int32
		kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			edgePage(http.StatusServiceUnavailable)(w)
		}, fastRetries())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p := params()
		p.IdempotencyKey = "order-90"
		start := time.Now()
		_, err := kc.SolveDeprecatedContext(ctx, p)
		elapsed := time.Since(start)

		e := kcErr(t, err)
		if e.Status != http.StatusServiceUnavailable || e.Code != "error" {
			t.Errorf("got status=%d code=%q, want the last edge 503, not a timeout", e.Status, e.Code)
		}
		if e.Attempts < 2 || int32(e.Attempts) != requests.Load() {
			t.Errorf("attempts=%d requests=%d, want at least one retry inside the window and the count to match", e.Attempts, requests.Load())
		}
		if elapsed > time.Second {
			t.Errorf("ran for %s; the window should have stopped the run long before the 5s deadline", elapsed)
		}
		for _, want := range []string{"gave up", fmt.Sprintf("%d attempts", e.Attempts), "over ", "HTTP 503"} {
			if !strings.Contains(e.Message, want) {
				t.Errorf("message %q does not mention %q", e.Message, want)
			}
		}
		if e.IdempotencyKey != "order-90" {
			t.Errorf("IdempotencyKey = %q, want order-90", e.IdempotencyKey)
		}
	})

	t.Run("the window does not bound the v2 submit", func(t *testing.T) {
		g := &retryGateway{submitQueue: []func(http.ResponseWriter){edge429, edge429, edge429}}
		kc := newClient(t, g.ServeHTTP, fastRetries())

		p := params()
		p.Deadline = 5 * time.Second
		if _, err := kc.Solve(p); err != nil {
			t.Fatalf("Solve: %v", err)
		}
		if submits, _ := g.counts(); submits != 4 {
			t.Errorf("submitted %d times, want 4 — v2 has no 120s claim to outlive, only the deadline", submits)
		}
	})
}

func TestKasadaStaysSingleShotWithoutAKey(t *testing.T) {
	// KasadaParams has no key field, so an edge 503 is final: a keyless resend of a request the
	// gateway may already be solving would be a second, billable solve.
	g := &keyedSolveGateway{queue: []func(http.ResponseWriter){edgePage(http.StatusServiceUnavailable)}}
	kc := newClient(t, g.ServeHTTP, fastRetries())

	_, err := kc.KasadaLogin(kasadaParams())
	e := kcErr(t, err)
	if e.Status != http.StatusServiceUnavailable || e.Attempts != 1 {
		t.Errorf("got status=%d attempts=%d, want 503 after 1", e.Status, e.Attempts)
	}
	if keys := g.seen(); len(keys) != 1 || keys[0] != "" {
		t.Errorf("keys = %v, want a single unkeyed request", keys)
	}
}

func TestSubmitRetriesSolveFailedOnTheV2Submit(t *testing.T) {
	// On v2 the verdict comes through the poll body; a 5xx on the submit means the job was not
	// taken, so even one labelled solve_failed is retried.
	g := &retryGateway{submitQueue: []func(http.ResponseWriter){
		status(http.StatusBadGateway, `{"success":false,"error":"solve_failed","message":"upstream"}`),
	}}
	kc := newClient(t, g.ServeHTTP, fastRetries())

	if _, err := kc.Solve(params()); err != nil {
		t.Fatalf("Solve: %v", err)
	}
	if submits, _ := g.counts(); submits != 2 {
		t.Errorf("submitted %d times, want 2", submits)
	}
}

func TestGetSolveWithoutADeadlineStillStopsRetrying(t *testing.T) {
	// GetSolve on context.Background() must not spin forever against a gateway that never
	// recovers; it gets DefaultSolveDeadline. MaxAttempts keeps the test short.
	var polls atomic.Int32
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		polls.Add(1)
		edge429(w)
	}, kagedcap.WithRetryPolicy(kagedcap.RetryPolicy{MaxAttempts: 4, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}))

	_, err := kc.GetSolve("job-x")
	e := kcErr(t, err)
	if e.Status != http.StatusTooManyRequests || e.Attempts != 4 || polls.Load() != 4 {
		t.Errorf("got status=%d attempts=%d polls=%d, want 429 after 4", e.Status, e.Attempts, polls.Load())
	}
}
