// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package harness

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

func (e *harnessExecutor) postManagedInput(ctx context.Context, c *openCodeClient, path string, body any) (result error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	// A newly attached reused session must be idle before this attempt admits
	// work. Cancellation during setup must not interrupt another caller's turn.
	if c.host.IsV2() {
		active, err := c.cleanupActive(ctx, c.sessionID)
		if err != nil {
			return err
		}
		if active {
			return errors.New("OpenCode session has unrelated active execution")
		}
	}
	e.mu.Lock()
	if err := ctx.Err(); err != nil {
		e.mu.Unlock()
		return err
	}
	if e.managedTurn != nil {
		e.managedTurn.submitted = true
	}
	e.mu.Unlock()
	resp, err := c.request(ctx, http.MethodPost, path, body)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, resp.Body.Close()) }()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		e.mu.Lock()
		if e.managedTurn != nil {
			e.managedTurn.submitted = false
		}
		e.mu.Unlock()
	}
	return fmt.Errorf("OpenCode input returned HTTP %d", resp.StatusCode)
}
