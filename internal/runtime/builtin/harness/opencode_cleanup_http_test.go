// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

package harness

import (
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type cleanupCloseFailure struct {
	io.ReadCloser
	failure error
	closed  *atomic.Int32
}

func (body *cleanupCloseFailure) Close() error {
	body.closed.Add(1)
	return errors.Join(body.ReadCloser.Close(), body.failure)
}

type cleanupCloseTransport struct {
	failure error
	closed  atomic.Int32
	path    string
}

func (transport *cleanupCloseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if transport.path != "" && request.URL.Path != transport.path {
		return response, nil
	}
	response.Body = &cleanupCloseFailure{ReadCloser: response.Body, failure: transport.failure, closed: &transport.closed}
	return response, nil
}

func TestCleanupCloseFailure(t *testing.T) {
	for _, operation := range []string{"interrupt", "activity", "owner", "wait", "input"} {
		t.Run(operation, func(t *testing.T) {
			fixture := newCancellationFixture(t)
			failure := errors.New("response body cleanup failed")
			transport := &cleanupCloseTransport{failure: failure}
			client := &openCodeClient{host: fixture.host, directory: "/workspace", http: &http.Client{Transport: transport}}
			switch operation {
			case "interrupt":
				interrupted, err := client.interrupt(t.Context(), "ses_test")
				require.True(t, interrupted)
				require.ErrorIs(t, err, failure)
			case "activity":
				busy, err := client.cleanupActive(t.Context(), "ses_test")
				require.False(t, busy)
				require.ErrorIs(t, err, failure)
			case "owner":
				pid, err := client.ownerPID(t.Context())
				require.Equal(t, 123, pid)
				require.ErrorIs(t, err, failure)
			case "wait":
				require.ErrorIs(t, client.awaitIdle(t.Context(), "ses_test"), failure)
			case "input":
				client.sessionID = "ses_test"
				transport.path = "/api/session/ses_test/prompt"
				err := fixture.executor.postManagedInput(t.Context(), client, "/session/ses_test/prompt", nil)
				require.True(t, fixture.busy.Load(), "the input was accepted before body cleanup failed")
				require.ErrorIs(t, err, failure)
			}
			require.Equal(t, int32(1), transport.closed.Load(), "a failed close must not permit further verification requests")
		})
	}
}
