// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package runtime

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/dagucloud/dagu/v2/internal/cmn/cmdutil"
	"github.com/dagucloud/dagu/v2/internal/ir"
	"github.com/stretchr/testify/require"
)

func TestRetryPreservesCleanupFence(t *testing.T) {
	step := ir.Step{Name: "coding"}
	node := NewNode(step, NodeState{})
	node.SetStatus(ir.NodeFailed)
	session := &ir.AgentSession{Provider: "opencode", SessionID: "ses_original", Generation: 4, PromptSent: true, HostInstanceID: "original-host", OwnerWorkerID: "worker", Directory: "/workspace", State: ir.AgentSessionAborted, LastError: "original cancellation", CleanupStatus: "unverified"}
	node.SetAgentSession(session)
	before := node.GetAgentSession()
	clearNodeForRetry(node, step)
	require.Equal(t, before, node.GetAgentSession())
	require.Equal(t, 4, node.GetAgentSession().Generation)
	err := NewStepExecutor().Execute(context.Background(), node)
	require.ErrorIs(t, err, ir.ErrAgentCleanupUnverified)
	require.ErrorIs(t, node.Error(), ir.ErrAgentCleanupUnverified)
	require.Equal(t, "ses_original", node.GetAgentSession().SessionID)
}

type cleanupTimeoutExecutor struct{ err error }

func TestRetryLegacyCleanupFence(t *testing.T) {
	for _, state := range []ir.AgentSessionState{ir.AgentSessionFailed, ir.AgentSessionAborted, ir.AgentSessionUnavailable} {
		t.Run(string(state), func(t *testing.T) {
			step := ir.Step{Name: "coding"}
			node := NewNode(step, NodeState{})
			node.SetStatus(ir.NodeFailed)
			node.SetAgentSession(&ir.AgentSession{Provider: "opencode", SessionID: "ses_original", Generation: 4, PromptSent: true, HostInstanceID: "original-host", OwnerWorkerID: "worker", Directory: "/workspace", OwnerPID: 123, State: state, LastError: "original failure"})
			before := node.GetAgentSession()
			clearNodeForRetry(node, step)
			require.Equal(t, before, node.GetAgentSession())
			err := NewStepExecutor().Execute(t.Context(), node)
			require.ErrorIs(t, err, ir.ErrAgentCleanupUnverified)
			require.Equal(t, before, node.GetAgentSession())
			before.CleanupStatus = "verified"
			node.SetAgentSession(before)
			node.SetStatus(ir.NodeFailed)
			clearNodeForRetry(node, step)
			require.Equal(t, 5, node.GetAgentSession().Generation)
			require.Empty(t, node.GetAgentSession().SessionID)
			require.False(t, node.GetAgentSession().CleanupBlocksExecution())
		})
	}
}

func (e cleanupTimeoutExecutor) Run(context.Context) error { return e.err }
func (e cleanupTimeoutExecutor) SetStdout(io.Writer)       {}
func (e cleanupTimeoutExecutor) SetStderr(io.Writer)       {}
func (e cleanupTimeoutExecutor) Kill(os.Signal) error      { return nil }

func TestTimeoutPreservesCleanupErrors(t *testing.T) {
	interrupt := errors.New("interrupt rejected")
	idle := errors.New("idle verification failed")
	node := NewNode(ir.Step{Name: "coding"}, NodeState{})
	ctx := t.Context()
	code, err := node.runCommand(ctx, cleanupTimeoutExecutor{err: errors.Join(context.DeadlineExceeded, interrupt, idle, ir.ErrAgentCleanupUnverified)}, time.Second)
	require.Equal(t, 124, code)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, interrupt)
	require.ErrorIs(t, err, idle)
	require.ErrorIs(t, err, ir.ErrAgentCleanupUnverified)
	require.ErrorIs(t, node.Error(), interrupt)
	require.ErrorIs(t, node.Error(), idle)
}

type cleanupStopExecutor struct {
	cleanupTimeoutExecutor
	session *ir.AgentSession
}

func (e *cleanupStopExecutor) GetAgentSession() *ir.AgentSession {
	return ir.CloneAgentSession(e.session)
}

func (e *cleanupStopExecutor) SetAgentSession(session *ir.AgentSession) { e.session = session }

func (e *cleanupStopExecutor) Stop(cmdutil.TerminationIntent) error {
	e.session.CleanupStatus = "unverified"
	return ir.ErrAgentCleanupUnverified
}

func TestStopCapturesCleanupFence(t *testing.T) {
	step := ir.Step{Name: "coding"}
	node := NewNode(step, NodeState{})
	node.SetStatus(ir.NodeRunning)
	exec := &cleanupStopExecutor{session: &ir.AgentSession{Provider: "opencode", SessionID: "ses_original", Generation: 4, PromptSent: true, State: ir.AgentSessionRunning}}
	node.SetAgentSession(exec.GetAgentSession())
	node.cmd = exec
	node.Stop(t.Context(), cmdutil.ForceTermination(), false)
	require.Equal(t, ir.NodeAborted, node.Status())
	require.True(t, node.GetAgentSession().CleanupBlocksExecution(), "Stop must capture the fence without relying on a blocked progress callback")
	clearNodeForRetry(node, step)
	require.Equal(t, 4, node.GetAgentSession().Generation)
	require.Equal(t, "ses_original", node.GetAgentSession().SessionID)
}

func TestLateProgressRetainsCleanupFence(t *testing.T) {
	node := NewNode(ir.Step{Name: "coding"}, NodeState{})
	node.SetAgentSession(&ir.AgentSession{Provider: "opencode", SessionID: "ses_original", Generation: 4, PromptSent: true, CleanupStatus: "unverified", CleanupIdleError: "cleanup deadline exceeded"})
	exec := &cleanupStopExecutor{session: &ir.AgentSession{Provider: "opencode", SessionID: "ses_original", Generation: 4, PromptSent: true, State: ir.AgentSessionRunning}}
	NewStepExecutor().captureLiveExecutorSideChannels(exec, node)
	require.True(t, node.GetAgentSession().CleanupBlocksExecution())
	require.Equal(t, "cleanup deadline exceeded", node.GetAgentSession().CleanupIdleError)
	require.Equal(t, 4, node.GetAgentSession().Generation)
}

type staleCleanupExecutor struct {
	*cleanupStopExecutor
	entered chan struct{}
	release chan struct{}
}

func (e *staleCleanupExecutor) GetAgentSession() *ir.AgentSession {
	snapshot := e.cleanupStopExecutor.GetAgentSession()
	if snapshot.CleanupStatus == "" {
		close(e.entered)
		<-e.release
	}
	return snapshot
}

func TestStopProgressSnapshotRace(t *testing.T) {
	node := NewNode(ir.Step{Name: "coding"}, NodeState{})
	node.SetStatus(ir.NodeRunning)
	exec := &staleCleanupExecutor{
		cleanupStopExecutor: &cleanupStopExecutor{session: &ir.AgentSession{Provider: "opencode", SessionID: "ses_original", Generation: 4, PromptSent: true, State: ir.AgentSessionRunning}},
		entered:             make(chan struct{}), release: make(chan struct{}),
	}
	node.cmd = exec
	node.SetAgentSession(exec.cleanupStopExecutor.GetAgentSession())
	done := make(chan struct{})
	go func() {
		NewStepExecutor().captureLiveExecutorSideChannels(exec, node)
		close(done)
	}()
	<-exec.entered
	node.Stop(t.Context(), cmdutil.ForceTermination(), false)
	close(exec.release)
	<-done
	require.True(t, node.GetAgentSession().CleanupBlocksExecution())
	require.Equal(t, "ses_original", node.GetAgentSession().SessionID)
	require.Equal(t, 4, node.GetAgentSession().Generation)
}

type pendingCleanupExecutor struct {
	*cleanupStopExecutor
	entered chan struct{}
	release chan struct{}
}

func (e *pendingCleanupExecutor) Stop(intent cmdutil.TerminationIntent) error {
	close(e.entered)
	<-e.release
	return e.cleanupStopExecutor.Stop(intent)
}

func TestStopFencesBeforeSettlement(t *testing.T) {
	node := NewNode(ir.Step{Name: "coding"}, NodeState{})
	node.SetStatus(ir.NodeRunning)
	exec := &pendingCleanupExecutor{
		cleanupStopExecutor: &cleanupStopExecutor{session: &ir.AgentSession{Provider: "opencode", SessionID: "ses_original", Generation: 4, State: ir.AgentSessionRunning}},
		entered:             make(chan struct{}), release: make(chan struct{}),
	}
	node.cmd = exec
	node.SetAgentSession(exec.GetAgentSession())
	done := make(chan struct{})
	go func() { node.Stop(t.Context(), cmdutil.ForceTermination(), false); close(done) }()
	<-exec.entered
	blocked := node.GetAgentSession().CleanupBlocksExecution()
	NewStepExecutor().captureLiveExecutorSideChannels(exec, node)
	retained := node.GetAgentSession().CleanupBlocksExecution()
	close(exec.release)
	<-done
	require.True(t, blocked, "local aborted status must not permit restart during the cleanup budget")
	require.True(t, retained, "a pre-Stop progress snapshot cannot erase pending cleanup")
	require.Equal(t, "unverified", node.GetAgentSession().CleanupStatus)
}
