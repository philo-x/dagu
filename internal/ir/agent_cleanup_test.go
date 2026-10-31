// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package ir

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCleanupFencesGeneration(t *testing.T) {
	for _, status := range []string{"requested", "acknowledged", "unverified", "unknown-future-status"} {
		t.Run(status, func(t *testing.T) {
			session := &AgentSession{Provider: "opencode", SessionID: "ses_original", Generation: 3, HostInstanceID: "original-host", OwnerWorkerID: "worker", Directory: "/workspace", OwnerPID: 123, CleanupStatus: status, CleanupInterruptError: "redacted interruption failure", CleanupIdleError: "redacted idle failure"}
			encoded, err := json.Marshal(session)
			require.NoError(t, err)
			var restored AgentSession
			require.NoError(t, json.Unmarshal(encoded, &restored))
			require.True(t, restored.CleanupBlocksExecution())
			clone := CloneAgentSession(&restored)
			before := CloneAgentSession(clone)
			clone.StartNewGeneration()
			require.Equal(t, before, clone)
			require.Equal(t, "ses_original", clone.SessionID)
			require.Equal(t, 3, clone.Generation)
			clone.CleanupStatus = "verified"
			clone.StartNewGeneration()
			require.Equal(t, 4, clone.Generation)
			require.Empty(t, clone.SessionID)
			require.False(t, clone.CleanupBlocksExecution())
		})
	}
}

func TestLegacyCleanupFencesGeneration(t *testing.T) {
	for _, state := range []AgentSessionState{AgentSessionFailed, AgentSessionAborted, AgentSessionUnavailable} {
		t.Run(string(state), func(t *testing.T) {
			// Persisted records predating cleanup evidence omit cleanupStatus.
			encoded := `{"provider":"opencode","sessionId":"ses_original","generation":3,"promptSent":true,"hostInstanceId":"original-host","ownerWorkerId":"worker","directory":"/workspace","ownerPid":123,"state":"` + string(state) + `"}`
			var session AgentSession
			require.NoError(t, json.Unmarshal([]byte(encoded), &session))
			before := CloneAgentSession(&session)
			blocked := session.CleanupBlocksExecution()
			session.StartNewGeneration()
			require.Equal(t, before, CloneAgentSession(&session))
			require.True(t, blocked)
			session.CleanupStatus = "verified"
			require.False(t, session.CleanupBlocksExecution())
			session.StartNewGeneration()
			require.Equal(t, 4, session.Generation)
			require.Empty(t, session.SessionID)
			require.Equal(t, "ses_original", session.DiscardedSessionID)
		})
	}
}

func TestLegacyCleanupCompatibility(t *testing.T) {
	for _, test := range []struct {
		name    string
		session *AgentSession
	}{
		{name: "nil"},
		{name: "succeeded", session: &AgentSession{Provider: "opencode", SessionID: "ses_old", PromptSent: true, State: AgentSessionSucceeded}},
		{name: "waiting", session: &AgentSession{Provider: "opencode", SessionID: "ses_old", PromptSent: true, State: AgentSessionWaiting}},
		{name: "running", session: &AgentSession{Provider: "opencode", SessionID: "ses_old", PromptSent: true, State: AgentSessionRunning}},
		{name: "starting", session: &AgentSession{Provider: "opencode", SessionID: "ses_old", PromptSent: true, State: AgentSessionStarting}},
		{name: "never-submitted", session: &AgentSession{Provider: "opencode", SessionID: "ses_old", State: AgentSessionFailed}},
		{name: "no-session", session: &AgentSession{Provider: "opencode", PromptSent: true, State: AgentSessionFailed}},
		{name: "other-provider", session: &AgentSession{Provider: "browser", SessionID: "ses_old", PromptSent: true, State: AgentSessionFailed}},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.False(t, test.session.CleanupBlocksExecution())
			if test.session == nil {
				return
			}
			test.session.StartNewGeneration()
			require.Equal(t, 2, test.session.Generation)
			require.Empty(t, test.session.SessionID)
		})
	}
}
