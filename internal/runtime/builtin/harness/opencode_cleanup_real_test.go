// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dagucloud/dagu/v2/internal/cmn/cmdutil"
	"github.com/dagucloud/dagu/v2/internal/cmn/config"
	"github.com/dagucloud/dagu/v2/internal/opencodehost"
	"github.com/dagucloud/dagu/v2/internal/runtime"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// The provider fixture supplies a real tool request to the real OpenCode server;
// it does not mock OpenCode, shell execution, interruption, or settlement.
func TestRealV2Cleanup(t *testing.T) {
	binary := os.Getenv("DAGU_TEST_OPENCODE_V2_EXECUTABLE")
	if binary == "" {
		t.Skip("DAGU_TEST_OPENCODE_V2_EXECUTABLE is not set")
	}
	ctx, cancel := context.WithTimeout(runtime.WithEnv(t.Context(), runtime.Env{WorkerID: "cleanup-worker"}), 60*time.Second)
	defer cancel()
	root := t.TempDir()
	effects := filepath.Join(root, "effects")
	var requested atomic.Bool
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Tools []struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			} `json:"tools"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			http.Error(w, "bad request", 400)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		tool := ""
		for _, item := range request.Tools {
			if item.Function.Name == "shell" {
				tool = item.Function.Name
			}
		}
		if tool != "" && !requested.Swap(true) {
			command := fmt.Sprintf("for i in $(seq 1 600); do echo tick >> %q; sleep 0.05; done", effects)
			arguments, err := json.Marshal(map[string]string{"command": command})
			if err != nil {
				t.Error(err)
				http.Error(w, "bad tool request", 500)
				return
			}
			chunk := map[string]any{"id": "cleanup", "object": "chat.completion.chunk", "created": 1, "model": "test", "choices": []any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"index": 0, "id": "call_cleanup", "type": "function", "function": map[string]any{"name": tool, "arguments": string(arguments)}}}}, "finish_reason": nil}}}
			data, err := json.Marshal(chunk)
			if err != nil {
				t.Error(err)
				http.Error(w, "bad response", 500)
				return
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
			_, _ = io.WriteString(w, `data: {"id":"cleanup","object":"chat.completion.chunk","created":1,"model":"test","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`+"\n\ndata: [DONE]\n\n")
			return
		}
		_, _ = io.WriteString(w, `data: {"id":"cleanup","object":"chat.completion.chunk","created":1,"model":"test","choices":[{"index":0,"delta":{"content":"done"},"finish_reason":"stop"}]}`+"\n\ndata: [DONE]\n\n")
	}))
	defer provider.Close()
	for _, key := range []string{"XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME", "OPENCODE_CONFIG_DIR"} {
		t.Setenv(key, filepath.Join(root, key))
	}
	t.Setenv("OPENCODE_CONFIG_PROJECT_DISABLE", "1")
	t.Setenv("OPENCODE_DISABLE_CLAUDE_CODE", "1")
	t.Setenv("OPENCODE_DISABLE_MODELS_FETCH", "1")
	settings := map[string]any{"model": "dagutest/test", "providers": map[string]any{"dagutest": map[string]any{"package": "@ai-sdk/openai-compatible", "settings": map[string]any{"baseURL": provider.URL + "/v1", "apiKey": "local-test"}, "models": map[string]any{"test": map[string]any{"name": "test"}}}}, "permissions": []any{map[string]string{"action": "shell", "resource": "*", "effect": "allow"}}}
	content, err := json.Marshal(settings)
	require.NoError(t, err)
	t.Setenv("OPENCODE_CONFIG_CONTENT", string(content))
	host := opencodehost.New(ctx, config.OpenCodeConfig{Executable: binary})
	defer func() {
		closing, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		require.NoError(t, host.Close(closing))
	}()
	owner, err := host.Ensure()
	require.NoError(t, err)
	location, err := exec.CommandContext(ctx, binary, "debug", "paths", "db").Output()
	require.NoError(t, err)
	database, err := filepath.EvalSymlinks(strings.TrimSpace(string(location)))
	require.NoError(t, err, "the isolated database must exist before opening it")
	isolated, err := filepath.EvalSymlinks(root)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(database, isolated+string(filepath.Separator)), "only the isolated test database may be locked")
	info, err := os.Stat(database)
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())
	store, err := sql.Open("sqlite", database)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	connection, err := store.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, connection.Close()) }()
	_, err = connection.ExecContext(ctx, "PRAGMA busy_timeout = 1000")
	require.NoError(t, err)
	var journal string
	require.NoError(t, connection.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal))
	require.Equal(t, "wal", journal, "the writer lock must leave owner reads available")
	var release sync.Once
	var releaseErr error
	committed := make(chan struct{})
	unlock := func() {
		release.Do(func() {
			commit, finish := context.WithTimeout(context.Background(), time.Second)
			defer finish()
			_, releaseErr = connection.ExecContext(commit, "COMMIT")
			close(committed)
		})
	}
	target, err := url.Parse(owner.URL)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(target)
	var acknowledged atomic.Int64
	var resumeDisabled, locked atomic.Bool
	proxy.ModifyResponse = func(response *http.Response) error {
		if strings.HasSuffix(response.Request.URL.Path, "/interrupt") && response.StatusCode == http.StatusOK {
			resumeDisabled.Store(response.Request.URL.Query().Get("resume") == "false")
			if acknowledged.CompareAndSwap(0, time.Now().UnixNano()) && locked.Load() {
				go func() {
					timer := time.NewTimer(500 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-timer.C:
						unlock()
					case <-committed:
					}
				}()
			}
		}
		return nil
	}
	transport := httptest.NewServer(proxy)
	defer func() { cancel(); transport.Close() }()
	proxied := owner
	proxied.URL = transport.URL
	e := &harnessExecutor{prompt: "Run the supplied tool", workDir: root}
	result := make(chan error, 1)
	go func() {
		_, err := e.runManagedOpenCode(ctx, providerConfig{flags: map[string]any{"model": "dagutest/test"}}, proxied)
		result <- err
	}()
	require.Eventually(t, func() bool { info, err := os.Stat(effects); return err == nil && info.Size() > 0 }, 20*time.Second, 50*time.Millisecond)
	// Delay real terminal persistence without changing API responses or shell cleanup.
	_, err = connection.ExecContext(ctx, "BEGIN IMMEDIATE")
	require.NoError(t, err)
	locked.Store(true)
	defer func() { unlock(); require.NoError(t, releaseErr) }()
	require.NoError(t, e.Stop(cmdutil.ForceTermination()))
	returned := time.Now()
	require.ErrorIs(t, <-result, context.Canceled)
	require.Positive(t, acknowledged.Load())
	require.True(t, resumeDisabled.Load())
	interval := returned.Sub(time.Unix(0, acknowledged.Load()))
	require.GreaterOrEqual(t, interval, 400*time.Millisecond, "Stop must wait beyond acknowledgment for delayed real owner settlement")
	select {
	case <-committed:
	default:
		t.Fatal("Stop returned before the isolated owner database transaction was released")
	}
	session := e.GetAgentSession()
	require.True(t, session.CleanupAcknowledged)
	require.True(t, session.CleanupInterrupted)
	require.Equal(t, "verified", session.CleanupStatus)
	require.Equal(t, owner.InstanceID, session.HostInstanceID)
	require.Equal(t, "cleanup-worker", session.OwnerWorkerID)
	require.Equal(t, root, session.Directory)
	require.Positive(t, session.OwnerPID)
	before, err := os.ReadFile(effects)
	require.NoError(t, err)
	time.Sleep(300 * time.Millisecond)
	after, err := os.ReadFile(effects)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after), "tool effects must cease beyond interruption acknowledgment and verified settlement")
	client := &openCodeClient{host: owner, directory: root, http: newOpenCodeHTTPClient(time.Second)}
	pid, err := client.ownerPID(ctx)
	require.NoError(t, err)
	require.Equal(t, session.OwnerPID, pid)
	active, err := client.cleanupActive(ctx, session.SessionID)
	require.NoError(t, err)
	require.False(t, active)
	t.Logf("OpenCode %s: acknowledged-to-Stop interval=%s, delayed owner database settlement verified, same owner pid=%d idle; effects unchanged for 300ms", owner.Version, interval, pid)
}
