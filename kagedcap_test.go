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
