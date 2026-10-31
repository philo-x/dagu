// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/dagucloud/dagu/v2/internal/ir"
)

const managedCleanupBudget = 3 * time.Second

type managedTurn struct {
	cancel          context.CancelFunc
	done            chan struct{}
	terminal        bool
	submitted       bool
	cleanupRequired bool
	cleanupErr      error
	cleanupDeadline time.Time
	cleanupTimedOut bool
}

// cleanupEvidence never displays response bodies, credentials, or transport URLs.
type cleanupEvidence struct {
	operation string
	err       error
}

func (e *cleanupEvidence) Error() string { return "OpenCode cleanup " + e.operation + " failed" }
func (e *cleanupEvidence) Unwrap() error { return e.err }
func cleanupFailure(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &cleanupEvidence{operation: operation, err: err}
}

func decodeCleanup(body io.Reader, target any) error {
	decoder := json.NewDecoder(io.LimitReader(body, 1024*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("invalid trailing cleanup response")
	}
	return nil
}

func (c *openCodeClient) interrupt(ctx context.Context, sessionID string) (interrupted bool, result error) {
	if !c.host.IsV2() {
		return true, c.json(ctx, http.MethodPost, "/session/"+url.PathEscape(sessionID)+"/abort", nil, nil)
	}
	resp, err := c.request(ctx, http.MethodPost, "/session/"+url.PathEscape(sessionID)+"/interrupt?resume=false", nil)
	if err != nil {
		return false, err
	}
	defer func() { result = errors.Join(result, resp.Body.Close()) }()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("interrupt returned HTTP %d", resp.StatusCode)
	}
	var body struct {
		Interrupted *bool `json:"interrupted"`
	}
	if err := decodeCleanup(resp.Body, &body); err != nil {
		return false, err
	}
	if body.Interrupted == nil {
		return false, errors.New("interrupt response missing interrupted boolean")
	}
	return *body.Interrupted, nil
}

func (c *openCodeClient) awaitIdle(ctx context.Context, sessionID string) error {
	if c.host.IsV2() {
		resp, err := c.request(ctx, http.MethodPost, "/experimental/session/"+url.PathEscape(sessionID)+"/wait", nil)
		if err != nil {
			return err
		}
		closing := resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			return errors.Join(fmt.Errorf("idle wait returned HTTP %d", resp.StatusCode), closing)
		}
		if closing != nil {
			return closing
		}
	}
	// Recheck process-owned activity after wait, including successors admitted
	// during cleanup. V1 has no settlement endpoint and uses its status poll.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		busy, err := c.cleanupActive(ctx, sessionID)
		if err != nil {
			return err
		}
		if !busy {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *openCodeClient) cleanupActive(ctx context.Context, sessionID string) (busy bool, result error) {
	resp, err := c.request(ctx, http.MethodGet, "/session/status", nil)
	if err != nil {
		return false, err
	}
	defer func() { result = errors.Join(result, resp.Body.Close()) }()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("activity returned HTTP %d", resp.StatusCode)
	}
	var statuses map[string]struct {
		Type string `json:"type"`
	}
	if c.host.IsV2() {
		var body struct {
			Data *map[string]struct {
				Type string `json:"type"`
			} `json:"data"`
		}
		if err := decodeCleanup(resp.Body, &body); err != nil {
			return false, err
		}
		if body.Data == nil {
			return false, errors.New("activity response missing data")
		}
		statuses = *body.Data
	} else {
		return cleanupActiveV1(resp.Body, sessionID)
	}
	if statuses == nil {
		return false, errors.New("activity response is null")
	}
	status, present := statuses[sessionID]
	if !present {
		return false, nil
	}
	if c.host.IsV2() && status.Type != "running" {
		return false, errors.New("invalid activity status")
	}
	if !c.host.IsV2() && status.Type != "idle" && status.Type != "busy" && status.Type != "retry" {
		return false, errors.New("invalid activity status")
	}
	return status.Type != "idle", nil
}

func cleanupActiveV1(body io.Reader, sessionID string) (bool, error) {
	var statuses map[string]json.RawMessage
	if err := decodeCleanup(body, &statuses); err != nil {
		return false, err
	}
	if statuses == nil {
		return false, errors.New("activity response is null")
	}
	raw, present := statuses[sessionID]
	if !present {
		return false, nil
	}
	var kind struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &kind); err != nil {
		return false, err
	}
	if kind.Type == "idle" || kind.Type == "busy" {
		if err := decodeCleanup(bytes.NewReader(raw), &kind); err != nil {
			return false, err
		}
		return kind.Type == "busy", nil
	}
	if kind.Type != "retry" {
		return false, errors.New("invalid activity status")
	}
	// OpenCode 1.18.11's retry variant includes required timing and optional action metadata.
	var retry struct {
		Type    string  `json:"type"`
		Attempt *int64  `json:"attempt"`
		Message *string `json:"message"`
		Next    *int64  `json:"next"`
		Action  *struct {
			Reason   *string `json:"reason"`
			Provider *string `json:"provider"`
			Title    *string `json:"title"`
			Message  *string `json:"message"`
			Label    *string `json:"label"`
			Link     *string `json:"link"`
		} `json:"action"`
	}
	if err := decodeCleanup(bytes.NewReader(raw), &retry); err != nil {
		return false, err
	}
	if retry.Attempt == nil || *retry.Attempt < 0 || retry.Message == nil || retry.Next == nil || *retry.Next < 0 {
		return false, errors.New("invalid retry activity")
	}
	if retry.Action != nil && (retry.Action.Reason == nil || retry.Action.Provider == nil || retry.Action.Title == nil || retry.Action.Message == nil || retry.Action.Label == nil) {
		return false, errors.New("invalid retry action")
	}
	return true, nil
}

func (c *openCodeClient) ownerPID(ctx context.Context) (pid int, result error) {
	resp, err := c.request(ctx, http.MethodGet, "/info", nil)
	if err != nil {
		return 0, err
	}
	defer func() { result = errors.Join(result, resp.Body.Close()) }()
	if resp.StatusCode != http.StatusOK {
		return 0, errors.New("owner identity unavailable")
	}
	var info struct {
		PID     int    `json:"pid"`
		Version string `json:"version"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&info); err != nil {
		return 0, err
	}
	if info.PID <= 0 || info.Version != c.host.Version {
		return 0, errors.New("invalid owner identity")
	}
	return info.PID, nil
}

func (e *harnessExecutor) reconcileManagedCancellation(c *openCodeClient, sessionID string, turn *managedTurn) error {
	e.mu.Lock()
	if turn.cleanupTimedOut {
		err := turn.cleanupErr
		e.mu.Unlock()
		return err
	}
	if turn.cleanupDeadline.IsZero() {
		turn.cleanupDeadline = time.Now().Add(managedCleanupBudget)
	}
	deadline := turn.cleanupDeadline
	pid := e.agentSession.OwnerPID
	e.agentSession.CleanupStatus = "requested"
	e.appendAgentEventLocked("cleanup", "requested", "Requesting foreground interruption on the owning OpenCode process")
	e.mu.Unlock()
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	e.notifyProgress()
	if err := ctx.Err(); err != nil {
		e.mu.Lock()
		e.timeoutManagedCleanupLocked(turn)
		result := turn.cleanupErr
		e.mu.Unlock()
		return result
	}
	var identityErr error
	if c.host.IsV2() {
		current, err := c.ownerPID(ctx)
		if err != nil {
			identityErr = err
		} else if pid == 0 || current != pid {
			identityErr = errors.New("OpenCode execution owner changed")
		}
	}
	interrupted, interruptErr := false, identityErr
	if identityErr == nil {
		interruptCtx, interruptCancel := context.WithTimeout(ctx, managedCleanupBudget/3)
		interrupted, interruptErr = c.interrupt(interruptCtx, sessionID)
		interruptCancel()
	}
	e.mu.Lock()
	e.agentSession.CleanupAcknowledged = interruptErr == nil
	e.agentSession.CleanupInterrupted = interrupted
	if interruptErr == nil && !turn.cleanupTimedOut {
		e.agentSession.CleanupStatus = "acknowledged"
		e.appendAgentEventLocked("cleanup", "acknowledged", "Interruption acknowledged; foreground settlement is not yet verified")
	}
	e.mu.Unlock()
	e.notifyProgress()
	idleErr := identityErr
	if idleErr == nil {
		idleErr = c.awaitIdle(ctx, sessionID)
	}
	if idleErr == nil && c.host.IsV2() {
		current, err := c.ownerPID(ctx)
		idleErr = err
		if err == nil && current != pid {
			idleErr = errors.New("OpenCode execution owner changed")
		}
	}
	interruptErr = cleanupFailure("interrupt", interruptErr)
	idleErr = cleanupFailure("idle verification", idleErr)
	e.mu.Lock()
	if turn.cleanupTimedOut {
		result := errors.Join(turn.cleanupErr, interruptErr, idleErr)
		e.mu.Unlock()
		return result
	}
	if idleErr != nil {
		e.agentSession.CleanupStatus = "unverified"
	}
	if interruptErr != nil {
		e.agentSession.CleanupInterruptError = interruptErr.Error()
	}
	if idleErr != nil {
		e.agentSession.CleanupIdleError = idleErr.Error()
	}
	e.mu.Unlock()
	// Persist a blocking snapshot before committing verified. A stalled callback
	// can finish late, but must not carry an unsafe verified snapshot past Stop.
	e.notifyProgress()
	e.mu.Lock()
	if turn.cleanupTimedOut || ctx.Err() != nil {
		e.timeoutManagedCleanupLocked(turn)
		result := errors.Join(turn.cleanupErr, interruptErr, idleErr)
		e.mu.Unlock()
		return result
	}
	if idleErr == nil {
		e.agentSession.CleanupStatus = "verified"
	}
	e.appendAgentEventLocked("cleanup", e.agentSession.CleanupStatus, "Cleanup concerns only the owning server process's foreground execution, not detached tools or other hosts")
	e.mu.Unlock()
	if idleErr != nil {
		return errors.Join(ir.ErrAgentCleanupUnverified, interruptErr, idleErr)
	}
	return interruptErr
}

// A late cleanup or progress callback must never clear a Stop deadline fence.
func (e *harnessExecutor) timeoutManagedCleanupLocked(turn *managedTurn) {
	if turn.terminal || turn.cleanupTimedOut {
		return
	}
	turn.cleanupTimedOut = true
	turn.cleanupErr = errors.Join(turn.cleanupErr, ir.ErrAgentCleanupUnverified, cleanupFailure("deadline", context.DeadlineExceeded))
	if !turn.submitted || e.agentSession == nil {
		return
	}
	e.agentSession.CleanupStatus = "unverified"
	e.agentSession.CleanupIdleError = "OpenCode cleanup deadline exceeded before settlement and progress completed"
	e.appendAgentEventLocked("cleanup", "unverified", e.agentSession.CleanupIdleError)
}

func (e *harnessExecutor) waitManagedStop(turn *managedTurn) error {
	e.mu.Lock()
	deadline := turn.cleanupDeadline
	e.mu.Unlock()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-turn.done:
	case <-timer.C:
		e.mu.Lock()
		e.timeoutManagedCleanupLocked(turn)
		e.mu.Unlock()
	}
	e.mu.Lock()
	err := turn.cleanupErr
	e.mu.Unlock()
	return err
}
