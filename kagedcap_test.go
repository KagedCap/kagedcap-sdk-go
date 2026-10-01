package kagedcap_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	kagedcap "github.com/kagedcap/kagedcap-sdk-go/v2"
)

// newClient points a Client at a stand-in gateway. Every v2 test drives the real poll loop
// against it, so the contract these handlers encode is the thing under test.
func newClient(t *testing.T, h http.HandlerFunc) *kagedcap.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return kagedcap.New("test-key", kagedcap.WithBaseURL(srv.URL))
}

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
	})

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

func TestSolveOmitsIdempotencyKeyAndCallbackURLWhenUnset(t *testing.T) {
	type capture struct {
		hasKey      bool
		hasCallback bool
	}
	got := make(chan capture, 1)
	kc := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			_, hasCallback := body["callback_url"]
			_, hasKey := r.Header["Idempotency-Key"]
			got <- capture{hasKey: hasKey, hasCallback: hasCallback}
			w.WriteHeader(http.StatusAccepted)
			io.WriteString(w, `{"success":true,"id":"job-1","status":"running"}`)
			return
		}
		io.WriteString(w, `{"success":true,"id":"job-1","status":"done","token":"tok"}`)
	})

	if _, err := kc.Solve(params()); err != nil {
		t.Fatalf("Solve: %v", err)
	}
	if c := <-got; c.hasKey || c.hasCallback {
		t.Errorf("sent Idempotency-Key=%v callback_url=%v, want neither", c.hasKey, c.hasCallback)
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
