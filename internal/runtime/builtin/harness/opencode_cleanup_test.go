// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dagucloud/dagu/v2/internal/cmn/cmdutil"
	"github.com/dagucloud/dagu/v2/internal/ir"
	"github.com/dagucloud/dagu/v2/internal/opencodehost"
	"github.com/dagucloud/dagu/v2/internal/runtime"
	"github.com/stretchr/testify/require"
)

func TestInterruptContract(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `true`, `{"interrupted":"true"}`, `{"interrupted":true} {}`} {
		t.Run(body, func(t *testing.T) {
			var explicit atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				explicit.Store(r.URL.Query().Get("resume") == "false")
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			client := &openCodeClient{host: opencodehost.Config{URL: server.URL, Version: "2.1.4"}, http: server.Client()}
			err := client.abort(t.Context(), "ses_test")
			require.Error(t, err, "HTTP success must validate acknowledgment")
			require.True(t, explicit.Load(), "terminal interruption explicitly disables resume")
		})
	}
}

type cancellationFixture struct {
	executor         *harnessExecutor
	host             opencodehost.Config
	busy             atomic.Bool
	interrupts       atomic.Int32
	pid              atomic.Int32
	input            func(http.ResponseWriter, *http.Request)
	interrupt        func(http.ResponseWriter, *http.Request)
	wait             func(http.ResponseWriter, *http.Request)
	setup            func(http.ResponseWriter, *http.Request)
	activity         func(http.ResponseWriter, *http.Request)
	shutdownListener func() error
	submitted        chan struct{}
}

func newCancellationFixture(t *testing.T) *cancellationFixture {
	t.Helper()
	f := &cancellationFixture{submitted: make(chan struct{})}
	f.pid.Store(123)
	f.executor = &harnessExecutor{workDir: "/workspace", prompt: "test", agentSession: &ir.AgentSession{Provider: "opencode", SessionID: "ses_test", Generation: 7}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/info":
			_, _ = fmt.Fprintf(w, `{"version":"2.1.4","pid":%d}`, f.pid.Load())
		case "/api/session/ses_test":
			_, _ = io.WriteString(w, `{"data":{"id":"ses_test","location":{"directory":"/workspace"}}}`)
		case "/api/event":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case "/api/session/ses_test/agent":
			if f.setup != nil {
				f.setup(w, r)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "/api/session/ses_test/prompt", "/api/session/ses_test/command":
			f.busy.Store(true)
			close(f.submitted)
			if f.input != nil {
				f.input(w, r)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "/api/session/ses_test/interrupt":
			f.interrupts.Add(1)
			if r.URL.Query().Get("resume") != "false" {
				t.Error("missing explicit resume=false")
			}
			if r.URL.Query().Get("location[directory]") != "/workspace" {
				t.Error("wrong cleanup directory")
			}
			if f.interrupt != nil {
				f.interrupt(w, r)
				return
			}
			_, _ = io.WriteString(w, `{"interrupted":true}`)
		case "/api/experimental/session/ses_test/wait":
			if f.wait != nil {
				f.wait(w, r)
				return
			}
			f.busy.Store(false)
			w.WriteHeader(http.StatusNoContent)
		case "/api/session/active":
			if f.activity != nil && f.interrupts.Load() > 0 {
				f.activity(w, r)
				return
			}
			if f.busy.Load() {
				_, _ = io.WriteString(w, `{"data":{"ses_test":{"type":"running"}}}`)
				return
			}
			_, _ = io.WriteString(w, `{"data":{}}`)
		case "/api/session/ses_test/inbox", "/api/session/ses_test/message":
			_, _ = io.WriteString(w, `{"data":[]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	f.host = opencodehost.Config{URL: server.URL, Version: "2.1.4", InstanceID: "original-host"}
	f.shutdownListener = server.Listener.Close
	return f
}

func TestCancellationReconciliation(t *testing.T) {
	for _, scenario := range []string{"delayed", "idle-noop", "rejected", "malformed", "lost-response", "lost-delayed-response", "busy", "persistent-activity", "malformed-activity", "malformed-wait", "unreachable", "wait-rejected", "changed-owner"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCancellationFixture(t)
			switch scenario {
			case "delayed":
				f.wait = func(w http.ResponseWriter, _ *http.Request) {
					time.Sleep(80 * time.Millisecond)
					f.busy.Store(false)
					w.WriteHeader(204)
				}
			case "idle-noop":
				f.interrupt = func(w http.ResponseWriter, _ *http.Request) {
					f.busy.Store(false)
					_, _ = io.WriteString(w, `{"interrupted":false}`)
				}
			case "rejected":
				f.interrupt = func(w http.ResponseWriter, _ *http.Request) {
					http.Error(w, "secret-provider-body", http.StatusForbidden)
				}
			case "malformed":
				f.interrupt = func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{}`) }
			case "lost-response":
				f.interrupt = func(w http.ResponseWriter, _ *http.Request) {
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = connection.Close()
				}
			case "busy":
				f.wait = func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }
			case "lost-delayed-response":
				f.interrupt = func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }
			case "persistent-activity":
				f.activity = func(w http.ResponseWriter, _ *http.Request) {
					_, _ = io.WriteString(w, `{"data":{"ses_test":{"type":"running"}}}`)
				}
			case "malformed-activity":
				f.activity = func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{}`) }
			case "malformed-wait":
				f.wait = func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{}`) }
			case "unreachable":
				f.interrupt = func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Connection", "close")
					if err := f.shutdownListener(); err != nil {
						t.Error(err)
					}
					_, _ = io.WriteString(w, `{"interrupted":true}`)
				}
			case "wait-rejected":
				f.interrupt = func(w http.ResponseWriter, _ *http.Request) {
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = connection.Close()
				}
				f.wait = func(w http.ResponseWriter, _ *http.Request) {
					http.Error(w, "unreachable owner", http.StatusServiceUnavailable)
				}
			case "changed-owner":
				f.interrupt = func(w http.ResponseWriter, _ *http.Request) {
					f.pid.Store(456)
					_, _ = io.WriteString(w, `{"interrupted":true}`)
				}
			}
			ctx, cancel := context.WithCancel(runtime.WithEnv(t.Context(), runtime.Env{WorkerID: "original-worker"}))
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := f.executor.runManagedOpenCode(ctx, providerConfig{flags: map[string]any{}}, f.host)
				result <- err
			}()
			<-f.submitted
			cancel()
			start := time.Now()
			err := <-result
			require.ErrorIs(t, err, context.Canceled)
			require.Less(t, time.Since(start), managedCleanupBudget+time.Second)
			session := f.executor.GetAgentSession()
			require.Equal(t, "ses_test", session.SessionID)
			require.Equal(t, 7, session.Generation)
			require.Equal(t, "original-host", session.HostInstanceID)
			require.Equal(t, "original-worker", session.OwnerWorkerID)
			require.Equal(t, 123, session.OwnerPID)
			require.NotContains(t, session.LastError, "secret-provider-body")
			require.EqualValues(t, 1, f.interrupts.Load())
			unverified := scenario == "busy" || scenario == "persistent-activity" || scenario == "malformed-activity" || scenario == "malformed-wait" || scenario == "unreachable" || scenario == "wait-rejected" || scenario == "changed-owner"
			require.Equal(t, unverified, session.CleanupBlocksExecution())
			require.Equal(t, unverified, errors.Is(err, ir.ErrAgentCleanupUnverified))
			if unverified {
				require.Equal(t, "unverified", session.CleanupStatus)
				session.StartNewGeneration()
				require.Equal(t, 7, session.Generation)
				require.Equal(t, "ses_test", session.SessionID)
				_, retryErr := f.executor.runManagedOpenCode(runtime.WithEnv(t.Context(), runtime.Env{}), providerConfig{}, opencodehost.Config{InstanceID: "replacement"})
				require.ErrorIs(t, retryErr, ir.ErrAgentCleanupUnverified)
				return
			}
			require.Equal(t, "verified", session.CleanupStatus)
			if scenario == "rejected" || scenario == "malformed" || scenario == "lost-response" || scenario == "lost-delayed-response" {
				require.NotEmpty(t, session.CleanupInterruptError)
				require.False(t, session.CleanupAcknowledged)
			}
		})
	}
}

func TestStopWaitsForCleanup(t *testing.T) {
	f := newCancellationFixture(t)
	acknowledged, release := make(chan struct{}), make(chan struct{})
	f.wait = func(w http.ResponseWriter, _ *http.Request) {
		close(acknowledged)
		<-release
		f.busy.Store(false)
		w.WriteHeader(204)
	}
	ctx := runtime.WithEnv(t.Context(), runtime.Env{})
	result := make(chan error, 1)
	go func() {
		_, err := f.executor.runManagedOpenCode(ctx, providerConfig{flags: map[string]any{}}, f.host)
		result <- err
	}()
	<-f.submitted
	stopped := make(chan error, 2)
	go func() { stopped <- f.executor.Stop(cmdutil.ForceTermination()) }()
	<-acknowledged
	go func() { stopped <- f.executor.Stop(cmdutil.ForceTermination()) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned before cleanup settlement")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-stopped)
	require.NoError(t, <-stopped)
	require.ErrorIs(t, <-result, context.Canceled)
	require.EqualValues(t, 1, f.interrupts.Load())
	require.Equal(t, "verified", f.executor.GetAgentSession().CleanupStatus)
}

func TestSetupCancellationDoesNotInterrupt(t *testing.T) {
	f := newCancellationFixture(t)
	f.busy.Store(true)
	entered := make(chan struct{})
	f.setup = func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	}
	ctx := runtime.WithEnv(t.Context(), runtime.Env{})
	result := make(chan error, 1)
	go func() {
		_, err := f.executor.runManagedOpenCode(ctx, providerConfig{flags: map[string]any{"agent": "build"}}, f.host)
		result <- err
	}()
	<-entered
	require.NoError(t, f.executor.Stop(cmdutil.ForceTermination()))
	require.ErrorIs(t, <-result, context.Canceled)
	require.EqualValues(t, 0, f.interrupts.Load())
	require.True(t, f.busy.Load(), "unrelated reused-session work must be untouched")
	require.Empty(t, f.executor.GetAgentSession().CleanupStatus)
}

func TestRejectedCommandDoesNotInterrupt(t *testing.T) {
	f := newCancellationFixture(t)
	f.input = func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "command rejected", 400) }
	ctx := runtime.WithEnv(t.Context(), runtime.Env{})
	_, err := f.executor.runManagedOpenCode(ctx, providerConfig{flags: map[string]any{"command": "invalid"}}, f.host)
	require.ErrorContains(t, err, "HTTP 400")
	require.NoError(t, f.executor.Stop(cmdutil.ForceTermination()))
	require.EqualValues(t, 0, f.interrupts.Load())
	require.True(t, f.busy.Load(), "rejected request must not abort another execution")
	require.Empty(t, f.executor.GetAgentSession().CleanupStatus)
}

func TestBusyReusedSessionDoesNotInterrupt(t *testing.T) {
	f := newCancellationFixture(t)
	f.busy.Store(true)
	_, err := f.executor.runManagedOpenCode(runtime.WithEnv(t.Context(), runtime.Env{}), providerConfig{flags: map[string]any{}}, f.host)
	require.ErrorContains(t, err, "unrelated active execution")
	require.NoError(t, f.executor.Stop(cmdutil.ForceTermination()))
	require.EqualValues(t, 0, f.interrupts.Load())
	require.True(t, f.busy.Load())
}

func TestReplacementOwnerIsFenced(t *testing.T) {
	for _, changed := range []string{"host", "worker", "directory"} {
		t.Run(changed, func(t *testing.T) {
			f := newCancellationFixture(t)
			original := &ir.AgentSession{Provider: "opencode", SessionID: "ses_test", Generation: 7, PromptSent: true, State: ir.AgentSessionRunning, HostInstanceID: "original-host", OwnerWorkerID: "original-worker", Directory: "/workspace", OwnerPID: 123}
			f.executor.SetAgentSession(original)
			host := f.host
			worker := "original-worker"
			if changed == "host" {
				host.InstanceID = "replacement"
			}
			if changed == "worker" {
				worker = "replacement"
			}
			if changed == "directory" {
				f.executor.workDir = "/replacement"
			}
			_, err := f.executor.runManagedOpenCode(runtime.WithEnv(t.Context(), runtime.Env{WorkerID: worker}), providerConfig{}, host)
			require.ErrorIs(t, err, ir.ErrAgentCleanupUnverified)
			session := f.executor.GetAgentSession()
			require.Equal(t, original.SessionID, session.SessionID)
			require.Equal(t, original.Generation, session.Generation)
			require.Equal(t, original.HostInstanceID, session.HostInstanceID)
			require.Equal(t, original.OwnerWorkerID, session.OwnerWorkerID)
			require.Equal(t, original.Directory, session.Directory)
			require.True(t, session.CleanupBlocksExecution())
			require.EqualValues(t, 0, f.interrupts.Load())
		})
	}
}

func TestCancellationTerminalRace(t *testing.T) {
	for _, winner := range []string{"completion", "timeout", "stop"} {
		t.Run(winner, func(t *testing.T) {
			f := newCancellationFixture(t)
			f.executor.agentSession.PromptMessageID = "msg_test"
			ctx, cancel := context.WithCancel(runtime.WithEnv(t.Context(), runtime.Env{}))
			defer cancel()
			if winner == "timeout" {
				deadline, deadlineCancel := context.WithTimeout(ctx, 50*time.Millisecond)
				defer deadlineCancel()
				ctx = deadline
			}
			entered, release := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/experimental/session/ses_test/wait" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				if r.URL.Path == "/api/session/active" {
					_, _ = io.WriteString(w, `{"data":{}}`)
					return
				}
				if r.URL.Path == "/api/info" {
					_, _ = io.WriteString(w, `{"version":"2.1.4","pid":123}`)
					return
				}
				close(entered)
				<-release
				_, _ = io.WriteString(w, `{"data":[{"id":"msg_test","type":"user"},{"id":"msg_answer","type":"assistant","finish":"stop","time":{"completed":1},"content":[{"type":"text","text":"done"}]}]}`)
			}))
			defer server.Close()
			client := &openCodeClient{host: opencodehost.Config{URL: server.URL, Version: "2.1.4"}, http: server.Client()}
			turn := &managedTurn{cancel: cancel, done: make(chan struct{})}
			f.executor.agentSession.OwnerPID = 123
			f.executor.managedTurn = turn
			result := make(chan error, 1)
			go func() {
				output, err := f.executor.finishManagedSuccess(ctx, client, "ses_test")
				if output != nil {
					_ = cleanupStdoutSpool(output)
				}
				result <- err
			}()
			<-entered
			stopped := make(chan error, 1)
			if winner == "stop" {
				go func() { stopped <- f.executor.Stop(cmdutil.ForceTermination()) }()
			}
			if winner != "completion" {
				<-ctx.Done()
			}
			close(release)
			err := <-result
			if winner != "completion" {
				require.ErrorIs(t, err, ctx.Err())
				require.False(t, turn.terminal)
				require.NotEqual(t, ir.AgentSessionSucceeded, f.executor.GetAgentSession().State)
				close(turn.done)
				if winner == "stop" {
					require.NoError(t, <-stopped)
				}
				return
			}
			require.NoError(t, err)
			require.True(t, turn.terminal)
			close(turn.done)
			require.NoError(t, f.executor.Stop(cmdutil.ForceTermination()))
			require.NoError(t, ctx.Err(), "late Stop must not cancel a committed success")
			require.Equal(t, ir.AgentSessionSucceeded, f.executor.GetAgentSession().State)
		})
	}
}

func TestAbortWaitsForSettlement(t *testing.T) {
	var waited atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/session/ses_test/interrupt":
			_, _ = io.WriteString(w, `{"interrupted":true}`)
		case "/api/experimental/session/ses_test/wait":
			time.Sleep(80 * time.Millisecond)
			waited.Store(true)
			w.WriteHeader(http.StatusNoContent)
		case "/api/session/active":
			_, _ = io.WriteString(w, `{"data":{}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	err := abortManagedOpenCode(t.Context(), opencodehost.Config{URL: server.URL, Version: "2.1.4"}, "ses_test", "/workspace")
	require.NoError(t, err)
	require.True(t, waited.Load(), "acknowledgment alone is not settlement")
}

func TestStopProgressDeadline(t *testing.T) {
	f := newCancellationFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var blocked atomic.Bool
	f.executor.SetProgressCallback(func() {
		if f.executor.GetAgentSession().CleanupStatus == "requested" && !blocked.Swap(true) {
			close(entered)
			<-release
		}
	})
	result := make(chan error, 1)
	go func() {
		_, err := f.executor.runManagedOpenCode(runtime.WithEnv(t.Context(), runtime.Env{}), providerConfig{}, f.host)
		result <- err
	}()
	<-f.submitted
	stopped := make(chan error, 1)
	go func() { stopped <- f.executor.Stop(cmdutil.ForceTermination()) }()
	<-entered
	select {
	case err := <-stopped:
		require.ErrorIs(t, err, ir.ErrAgentCleanupUnverified)
		require.Equal(t, "unverified", f.executor.GetAgentSession().CleanupStatus)
	case <-time.After(managedCleanupBudget + 500*time.Millisecond):
		close(release)
		<-stopped
		<-result
		t.Fatal("Stop exceeded its budget with stalled progress persistence")
	}
	close(release)
	require.ErrorIs(t, <-result, ir.ErrAgentCleanupUnverified)
	require.True(t, f.executor.GetAgentSession().CleanupBlocksExecution(), "late cleanup cannot clear the timeout fence")
	require.EqualValues(t, 0, f.interrupts.Load(), "expired cleanup cannot issue another request")
}

func TestStopWaitingSession(t *testing.T) {
	f := newCancellationFixture(t)
	upstream, err := url.Parse(f.host.URL)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.FlushInterval = -1
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/permission/request" && f.busy.Load() {
			_, _ = io.WriteString(w, `{"data":[{"id":"per_test","sessionID":"ses_test","action":"shell","resources":["*"]}]}`)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	defer server.Close()
	f.host.URL = server.URL
	output, err := f.executor.runManagedOpenCode(runtime.WithEnv(t.Context(), runtime.Env{}), providerConfig{}, f.host)
	if output != nil {
		_ = cleanupStdoutSpool(output)
	}
	require.NoError(t, err)
	require.Equal(t, ir.AgentSessionWaiting, f.executor.GetAgentSession().State)
	require.NoError(t, f.executor.Stop(cmdutil.ForceTermination()))
	require.False(t, f.busy.Load())
	session := f.executor.GetAgentSession()
	require.Equal(t, "verified", session.CleanupStatus)
	require.Equal(t, "ses_test", session.SessionID)
	require.Equal(t, 123, session.OwnerPID)
	require.True(t, session.CleanupInterrupted)
	require.EqualValues(t, 1, f.interrupts.Load())
}

func TestStopFinalProgressDeadline(t *testing.T) {
	f := newCancellationFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	final, finish := make(chan struct{}), make(chan struct{})
	var acknowledgments atomic.Int32
	var persisted atomic.Pointer[ir.AgentSession]
	f.executor.SetProgressCallback(func() {
		snapshot := f.executor.GetAgentSession()
		if snapshot.CleanupStatus == "acknowledged" && acknowledgments.Add(1) == 2 {
			close(entered)
			<-release
		}
		if snapshot.CleanupStatus == "unverified" && acknowledgments.Load() >= 2 {
			close(final)
			<-finish
		}
		persisted.Store(snapshot)
	})
	result := make(chan error, 1)
	go func() {
		_, err := f.executor.runManagedOpenCode(runtime.WithEnv(t.Context(), runtime.Env{}), providerConfig{}, f.host)
		result <- err
	}()
	<-f.submitted
	stopped := make(chan error, 1)
	go func() { stopped <- f.executor.Stop(cmdutil.ForceTermination()) }()
	<-entered
	select {
	case err := <-stopped:
		require.ErrorIs(t, err, ir.ErrAgentCleanupUnverified)
	case <-time.After(managedCleanupBudget + 500*time.Millisecond):
		close(release)
		<-stopped
		<-result
		t.Fatal("Stop exceeded its budget during final progress persistence")
	}
	require.True(t, persisted.Load().CleanupBlocksExecution(), "persisted progress cannot imply idle before cleanup commit")
	close(release)
	<-final
	for range 5 {
		require.ErrorIs(t, f.executor.Stop(cmdutil.ForceTermination()), ir.ErrAgentCleanupUnverified)
	}
	require.EqualValues(t, 1, f.interrupts.Load(), "a stalled terminal callback cannot spawn another cleanup worker")
	close(finish)
	require.ErrorIs(t, <-result, ir.ErrAgentCleanupUnverified)
	require.Equal(t, "unverified", persisted.Load().CleanupStatus, "late persistence retains the timeout fence")
	require.True(t, f.executor.GetAgentSession().CleanupBlocksExecution())
}

// The second local terminal notification follows close(done), not remote settlement.
func TestStopLocallyClosedTurn(t *testing.T) {
	for _, state := range []ir.AgentSessionState{ir.AgentSessionWaiting, ir.AgentSessionFailed} {
		for _, settled := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/settled=%t", state, settled), func(t *testing.T) {
				f := newCancellationFixture(t)
				if state == ir.AgentSessionFailed {
					f.executor.agentSession.PromptSent = true
					f.executor.agentSession.OwnerPID = 123
					f.busy.Store(true)
				}
				upstream, err := url.Parse(f.host.URL)
				require.NoError(t, err)
				proxy := httputil.NewSingleHostReverseProxy(upstream)
				proxy.FlushInterval = -1
				var lookups atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if state == ir.AgentSessionFailed && r.URL.Path == "/api/info" && lookups.Add(1) == 1 {
						http.Error(w, "owner monitoring failed", http.StatusBadRequest)
						return
					}
					if state == ir.AgentSessionWaiting && r.URL.Path == "/api/event" {
						w.Header().Set("Content-Type", "text/event-stream")
						w.WriteHeader(http.StatusOK)
						w.(http.Flusher).Flush()
						select {
						case <-f.submitted:
						case <-r.Context().Done():
							return
						}
						_, _ = io.WriteString(w, "data: {\"type\":\"permission.asked\",\"data\":{\"sessionID\":\"ses_test\"}}\n\n")
						w.(http.Flusher).Flush()
						<-r.Context().Done()
						return
					}
					if r.URL.Path == "/api/permission/request" && f.busy.Load() {
						_, _ = io.WriteString(w, `{"data":[{"id":"per_test","sessionID":"ses_test","action":"shell","resources":["*"]}]}`)
						return
					}
					proxy.ServeHTTP(w, r)
				}))
				defer server.Close()
				f.host.URL = server.URL
				entered, release := make(chan struct{}), make(chan struct{})
				waiting, settle := make(chan struct{}), make(chan struct{})
				defer func() {
					for _, barrier := range []chan struct{}{release, settle} {
						select {
						case <-barrier:
						default:
							close(barrier)
						}
					}
				}()
				f.wait = func(w http.ResponseWriter, r *http.Request) {
					close(waiting)
					select {
					case <-settle:
					case <-r.Context().Done():
						return
					}
					if !settled {
						http.Error(w, "owner settlement unavailable", http.StatusServiceUnavailable)
						return
					}
					f.busy.Store(false)
					w.WriteHeader(http.StatusNoContent)
				}
				var notifications atomic.Int32
				f.executor.SetProgressCallback(func() {
					if f.executor.GetAgentSession().State == state && notifications.Add(1) == 2 {
						close(entered)
						<-release
					}
				})
				result := make(chan error, 1)
				go func() {
					output, err := f.executor.runManagedOpenCode(runtime.WithEnv(t.Context(), runtime.Env{WorkerID: "original-worker"}), providerConfig{}, f.host)
					if output != nil {
						_ = cleanupStdoutSpool(output)
					}
					result <- err
				}()
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("final local notification did not reach the barrier")
				}
				f.executor.mu.Lock()
				old := f.executor.managedTurn
				terminal := old.terminal
				f.executor.mu.Unlock()
				require.True(t, terminal)
				select {
				case <-old.done:
				default:
					t.Fatal("local done must be closed before Stop")
				}
				start := time.Now()
				stopped := make(chan error, 7)
				go func() { stopped <- f.executor.Stop(cmdutil.ForceTermination()) }()
				select {
				case err := <-stopped:
					t.Fatalf("Stop acknowledged locally closed turn while owner is busy: %v", err)
				case <-waiting:
				case <-time.After(time.Second):
					t.Fatal("Stop did not reconcile the retained owner")
				}
				f.executor.mu.Lock()
				cleanup := f.executor.managedTurn
				cancel := cleanup.cancel
				deadline := cleanup.cleanupDeadline
				joined := make(chan struct{}, 7)
				cleanup.cancel = func() {
					cancel()
					joined <- struct{}{}
				}
				f.executor.mu.Unlock()
				require.NotSame(t, old, cleanup)
				for range 5 {
					go func() { stopped <- f.executor.Stop(cmdutil.ForceTermination()) }()
				}
				for range 5 {
					select {
					case <-joined:
					case <-time.After(time.Second):
						t.Fatal("concurrent Stop did not join the retained cleanup turn")
					}
				}
				close(release)
				runErr := <-result
				if state == ir.AgentSessionFailed {
					require.ErrorIs(t, runErr, ir.ErrAgentCleanupUnverified)
				} else {
					require.NoError(t, runErr)
				}
				f.executor.mu.Lock()
				retained := f.executor.managedTurn
				remaining := cleanup.cleanupDeadline
				f.executor.mu.Unlock()
				require.Same(t, cleanup, retained, "old finalizer must not erase cleanup ownership")
				require.Equal(t, deadline, remaining, "repeated Stops must not renew the cleanup budget")
				go func() { stopped <- f.executor.Stop(cmdutil.ForceTermination()) }()
				select {
				case <-joined:
				case <-time.After(time.Second):
					t.Fatal("Stop after the old notification did not retain cleanup ownership")
				}
				close(settle)
				for range 7 {
					err := <-stopped
					if settled {
						require.NoError(t, err)
						continue
					}
					require.ErrorIs(t, err, ir.ErrAgentCleanupUnverified)
				}
				require.Less(t, time.Since(start), managedCleanupBudget+500*time.Millisecond)
				session := f.executor.GetAgentSession()
				require.Equal(t, !settled, session.CleanupBlocksExecution())
				require.Equal(t, !settled, f.busy.Load())
				require.Equal(t, "ses_test", session.SessionID)
				require.Equal(t, 7, session.Generation)
				require.Equal(t, 123, session.OwnerPID)
				require.Equal(t, "original-host", session.HostInstanceID)
				require.Equal(t, "original-worker", session.OwnerWorkerID)
				require.EqualValues(t, 1, f.interrupts.Load(), "concurrent Stops must share one cleanup worker")
				if !settled {
					require.NotEmpty(t, session.CleanupIdleError)
					require.ErrorIs(t, f.executor.Run(runtime.WithEnv(t.Context(), runtime.Env{})), ir.ErrAgentCleanupUnverified)
				}
			})
		}
	}
}

func TestAcceptedMonitoringFailure(t *testing.T) {
	f := newCancellationFixture(t)
	f.activity = func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "monitor failure", 500) }
	upstream, err := url.Parse(f.host.URL)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.FlushInterval = -1
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/session/active" && f.busy.Load() {
			http.Error(w, "monitor failure", 500)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	defer server.Close()
	f.host.URL = server.URL
	_, err = f.executor.runManagedOpenCode(runtime.WithEnv(t.Context(), runtime.Env{}), providerConfig{}, f.host)
	require.Error(t, err)
	require.True(t, f.executor.GetAgentSession().CleanupBlocksExecution())
	require.True(t, f.busy.Load(), "uncertain foreground work is retained behind a fence")
	require.EqualValues(t, 0, f.interrupts.Load())
	require.ErrorIs(t, err, ir.ErrAgentCleanupUnverified)
}

func TestResumedOwnerLookupFailure(t *testing.T) {
	f := newCancellationFixture(t)
	f.executor.agentSession.PromptSent = true
	f.executor.agentSession.State = ir.AgentSessionRunning
	f.executor.agentSession.OwnerPID = 123
	f.busy.Store(true)
	upstream, err := url.Parse(f.host.URL)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/info" {
			http.Error(w, "owner lookup failed", 500)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	defer server.Close()
	f.host.URL = server.URL
	_, err = f.executor.runManagedOpenCode(runtime.WithEnv(t.Context(), runtime.Env{}), providerConfig{}, f.host)
	require.ErrorIs(t, err, ir.ErrAgentCleanupUnverified)
	require.True(t, f.busy.Load())
	require.True(t, f.executor.GetAgentSession().CleanupBlocksExecution())
	require.Equal(t, 7, f.executor.GetAgentSession().Generation)
	require.EqualValues(t, 0, f.interrupts.Load())
}

func TestV1CleanupRetryStatus(t *testing.T) {
	for _, body := range []string{
		`{"other":{"type":"retry","attempt":1,"message":"rate limited","next":123}}`,
		`{"ses_test":{"type":"retry","attempt":1,"message":"rate limited","next":123}}`,
		`{"ses_test":{"type":"retry"}}`,
		`{"ses_test":{"type":"unknown"}}`,
		`{"ses_test":{"type":"idle","attempt":1}}`,
	} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) }))
			defer server.Close()
			client := &openCodeClient{host: opencodehost.Config{URL: server.URL, Version: "1.18.11"}, http: server.Client()}
			active, err := client.cleanupActive(t.Context(), "ses_test")
			switch body {
			case `{"other":{"type":"retry","attempt":1,"message":"rate limited","next":123}}`:
				require.NoError(t, err)
				require.False(t, active)
			case `{"ses_test":{"type":"retry","attempt":1,"message":"rate limited","next":123}}`:
				require.NoError(t, err)
				require.True(t, active)
			default:
				require.Error(t, err)
			}
		})
	}
}
