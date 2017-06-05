//  Copyright (c) 2017 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package middleware

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/net/context"

	"github.com/troubling/hummingbird/common/test"
)

func newFakeProxyContext() *ProxyContext {
	return &ProxyContext{
		Logger:                 zap.NewNop(),
		ProxyContextMiddleware: &ProxyContextMiddleware{Cache: &test.FakeMemcacheRing{}},
	}
}

func checkHeaders(t *testing.T, headers map[string]string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for key, expected := range headers {
			if v := r.Header.Get(key); v != expected {
				t.Errorf("Expected header %s to be %q, got %q", key, expected, v)
			}
		}
		w.Write([]byte("stuff"))
	})
}

func fakeIdentityServer(statusPost int, statusGet int, body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.WriteHeader(statusPost)
		} else if r.Method == "GET" {
			w.WriteHeader(statusGet)
		}
		io.WriteString(w, body)
	}))
}

func TestSpoofBlock(t *testing.T) {
	rec := httptest.NewRecorder()
	fakeContext := newFakeProxyContext()
	req, err := http.NewRequest("GET", "/someurl", nil)
	require.Nil(t, err)
	req.Header.Add("X-Identity-Status", "Confirmed")
	req.Header.Add("X-Project-Id", "134")
	req.Header.Add("X-Project-Name", "value")
	req.Header.Add("X-Domain-Id", "142")

	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", fakeContext))

	passthrough := checkHeaders(t, map[string]string{
		"X-Identity-Status": "Invalid",
		"X-Project-Name":    "",
		"X-Project-Id":      "",
		"X-Domain-Id":       "",
	})
	at := &authToken{
		next: passthrough,
	}
	at.ServeHTTP(rec, req)
	if body := rec.Body.String(); body != "stuff" {
		t.Errorf("Wrong body, got %q want %q", body, "stuff")
	}
}

func TestNoToken(t *testing.T) {
	rec := httptest.NewRecorder()
	fakeContext := newFakeProxyContext()
	req, err := http.NewRequest("GET", "/someurl", nil)
	require.Nil(t, err)

	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", fakeContext))

	passthrough := checkHeaders(t, map[string]string{
		"X-Identity-Status": "Invalid",
	})
	at := &authToken{
		next: passthrough,
	}
	at.ServeHTTP(rec, req)
	if body := rec.Body.String(); body != "stuff" {
		t.Errorf("Wrong body, got %q want %q", body, "stuff")
	}

}

type mockTokenMemacacheRing struct {
	MockValues map[string]interface{}
}

func (mr *mockTokenMemacacheRing) Decr(key string, delta int64, timeout int) (int64, error) {
	return int64(0), nil
}

func (mr *mockTokenMemacacheRing) Delete(key string) error {
	return nil
}

func (mr *mockTokenMemacacheRing) Get(key string) (interface{}, error) {
	if val, ok := mr.MockValues[key]; ok {
		return val, nil
	}
	return nil, errors.New("Some error")
}

func (mr *mockTokenMemacacheRing) GetStructured(key string, val interface{}) error {
	return nil
}

func (mr *mockTokenMemacacheRing) GetMulti(serverKey string, keys []string) (map[string]interface{}, error) {
	return nil, nil
}

func (mr *mockTokenMemacacheRing) Incr(key string, delta int64, timeout int) (int64, error) {
	return int64(0), nil
}

func (mr *mockTokenMemacacheRing) Set(key string, value interface{}, timeout int) error {
	mr.MockValues[key] = value
	return nil
}

func (mr *mockTokenMemacacheRing) SetMulti(serverKey string, values map[string]interface{}, timeout int) error {
	return nil
}

func TestExpiredToken(t *testing.T) {
	rec := httptest.NewRecorder()
	fakeCache := mockTokenMemacacheRing{MockValues: make(map[string]interface{})}
	fakeContext := &ProxyContext{
		Logger:                 zap.NewNop(),
		ProxyContextMiddleware: &ProxyContextMiddleware{Cache: &fakeCache},
	}
	obs, logs := observer.New(zap.DebugLevel)
	logger := zap.New(obs)
	fakeContext.Logger = logger
	req, err := http.NewRequest("GET", "/someurl", nil)
	require.Nil(t, err)
	req.Header.Set("X-Auth-Token", "abcd")
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", fakeContext))
	identityServ := fakeIdentityServer(201, 200, `
	{
    "token": {
        "audit_ids": [
            "_qJlMw8hSB2mBb5pAQn38w"
        ],
        "expires_at": "2015-05-21T08:52:02.283669Z",
        "issued_at": "2015-05-21T07:52:02.283716Z",
        "methods": [
            "password"
        ],
        "project": {
            "domain": {
                "id": "default",
                "name": "Default"
            },
            "id": "12f1b2e6285f4c0c8ee01f3a7d5c1bf1",
            "name": "test"
        },
        "roles": [
            {
                "id": "c5f9f21624c444bcaed7a4713b0e3003",
                "name": "admin"
            }
        ],
        "user": {
            "domain": {
                "id": "default",
                "name": "Default"
            },
            "id": "9ca318a4fdf244e19da9d1574b452bb1",
            "name": "tester"
        }
    }
}`)
	defer identityServ.Close()
	passthrough := checkHeaders(t, map[string]string{
		"X-Identity-Status": "Invalid",
		"X-Project-Name":    "",
		"X-Project-Id":      "",
		"X-Domain-Id":       "",
	})
	at := &authToken{
		next: passthrough,
		identity: &identity{authURL: identityServ.URL,
			client: &http.Client{
				Timeout: 5 * time.Second,
			}},
	}
	at.ServeHTTP(rec, req)
	require.Equal(t, 0, len(fakeCache.MockValues))
	require.Equal(t, 1, logs.Len())
	want := []observer.LoggedEntry{{
		Entry:   zapcore.Entry{Level: zap.DebugLevel, Message: "Failed to validate token"},
		Context: []zapcore.Field{zap.Error(errors.New("Returned token is not valid"))}}}
	require.Equal(t, want[0], logs.AllUntimed()[0])
	if rec.Code != 200 {
		t.Fatalf("wrong code, got %d want %d", rec.Code, 200)
	}
	if body := rec.Body.String(); body != "stuff" {
		t.Errorf("Wrong body, got %q want %q", body, "stuff")
	}
}

func TestUnscopedToken(t *testing.T) {
	rec := httptest.NewRecorder()
	fakeCache := mockTokenMemacacheRing{MockValues: make(map[string]interface{})}
	fakeContext := &ProxyContext{
		Logger:                 zap.NewNop(),
		ProxyContextMiddleware: &ProxyContextMiddleware{Cache: &fakeCache},
	}
	req, err := http.NewRequest("GET", "/someurl", nil)
	require.Nil(t, err)
	req.Header.Set("X-Auth-Token", "abcd")
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", fakeContext))
	identityServ := fakeIdentityServer(201, 200, `
	{
    "token": {
        "audit_ids": [
            "vtno99i8R4OLTLN9vByC-g"
        ],
        "expires_at": "2100-05-21T10:57:25.392438Z",
        "issued_at": "2017-05-21T09:57:25.392463Z",
        "methods": [
            "password"
        ],
        "project": {
            "domain": {
                "id": "default",
                "name": "Default"
            },
            "id": "37f58fe3bbc1407fa0b5443250ea0f9f",
            "name": "service"
        },
        "roles": [
            {
                "id": "c5f9f21624c444bcaed7a4713b0e3003",
                "name": "admin"
            }
        ],
        "user": {
            "domain": {
                "id": "default",
                "name": "Default"
            },
            "id": "921880ba817d438c9577c1932b6a169e",
            "name": "swift"
        }
    }
}`)
	defer identityServ.Close()
	passthrough := checkHeaders(t, map[string]string{
		"X-Identity-Status":  "Confirmed",
		"X-User-Id":          "921880ba817d438c9577c1932b6a169e",
		"X-User-Name":        "swift",
		"X-User-Domain-Id":   "default",
		"X-User-Domain-Name": "Default",
		"X-Roles":            "admin",
	})
	at := &authToken{
		next: passthrough,
		identity: &identity{authURL: identityServ.URL,
			client: &http.Client{
				Timeout: 5 * time.Second,
			}},
	}
	at.ServeHTTP(rec, req)
	require.Equal(t, 1, len(fakeCache.MockValues))
	if rec.Code != 200 {
		t.Fatalf("wrong code, got %d want %d", rec.Code, 200)
	}
	if body := rec.Body.String(); body != "stuff" {
		t.Errorf("Wrong body, got %q want %q", body, "stuff")
	}
}

func TestProjectScopedToken(t *testing.T) {
	rec := httptest.NewRecorder()
	fakeCache := mockTokenMemacacheRing{MockValues: make(map[string]interface{})}
	fakeContext := &ProxyContext{
		Logger:                 zap.NewNop(),
		ProxyContextMiddleware: &ProxyContextMiddleware{Cache: &fakeCache},
	}
	req, err := http.NewRequest("GET", "/someurl", nil)
	require.Nil(t, err)
	req.Header.Set("X-Auth-Token", "abcd")
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", fakeContext))
	identityServ := fakeIdentityServer(201, 200, `
	{
    "token": {
        "audit_ids": [
            "1JlsU26ZThSxeILfNWUZ6Q"
        ],
        "expires_at": "2100-05-21T11:08:19.270008Z",
        "issued_at": "2017-05-21T10:08:19.270034Z",
        "methods": [
            "password"
        ],
        "project": {
            "domain": {
                "id": "default",
                "name": "Default"
            },
            "id": "37f58fe3bbc1407fa0b5443250ea0f9f",
            "name": "service"
        },
        "roles": [
            {
                "id": "c5f9f21624c444bcaed7a4713b0e3003",
                "name": "admin"
            }
        ],
        "user": {
            "domain": {
                "id": "default",
                "name": "Default"
            },
            "id": "921880ba817d438c9577c1932b6a169e",
            "name": "swift"
        }
    }
}`)
	defer identityServ.Close()
	passthrough := checkHeaders(t, map[string]string{
		"X-Identity-Status":     "Confirmed",
		"X-Domain-Id":           "",
		"X-Domain-Name":         "",
		"X-Project-Name":        "service",
		"X-Project-Id":          "37f58fe3bbc1407fa0b5443250ea0f9f",
		"X-Project-Domain-Name": "Default",
		"X-Project-Domain-Id":   "default",
		"X-Roles":               "admin",
	})
	at := &authToken{
		next: passthrough,
		identity: &identity{authURL: identityServ.URL,
			client: &http.Client{
				Timeout: 5 * time.Second,
			}},
	}
	at.ServeHTTP(rec, req)
	require.Equal(t, 1, len(fakeCache.MockValues))
	if rec.Code != 200 {
		t.Fatalf("wrong code, got %d want %d", rec.Code, 200)
	}
	if body := rec.Body.String(); body != "stuff" {
		t.Errorf("Wrong body, got %q want %q", body, "stuff")
	}
}

func TestDomainScopedToken(t *testing.T) {
	rec := httptest.NewRecorder()
	fakeCache := mockTokenMemacacheRing{MockValues: make(map[string]interface{})}
	fakeContext := &ProxyContext{
		Logger:                 zap.NewNop(),
		ProxyContextMiddleware: &ProxyContextMiddleware{Cache: &fakeCache},
	}
	req, err := http.NewRequest("GET", "/someurl", nil)
	require.Nil(t, err)
	req.Header.Set("X-Auth-Token", "abcd")
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", fakeContext))
	identityServ := fakeIdentityServer(201, 200, `
	{
  "token": {
    "domain": {
      "id": "default",
      "name": "Default"
    },
    "methods": ["password"],
    "roles": [{
      "id": "c703057be878458588961ce9a0ce686b",
      "name": "admin"
    }],
    "expires_at": "2100-06-10T21:52:58.852167Z",
    "extras": {},
    "user": {
      "domain": {
        "id": "default",
        "name": "Default"
      },
      "id": "3ec3164f750146be97f21559ee4d9c51",
      "name": "admin"
    },
    "audit_ids": ["Xpa6Uyn-T9S6mTREudUH3w"],
    "issued_at": "2014-06-10T20:52:58.852194Z"
  }
}`)
	defer identityServ.Close()
	passthrough := checkHeaders(t, map[string]string{
		"X-Identity-Status": "Confirmed",
		"X-Project-Id":      "",
		"X-Domain-Id":       "default",
		"X-Domain-Name":     "Default",
		"X-Roles":           "admin",
	})
	at := &authToken{
		next: passthrough,
		identity: &identity{authURL: identityServ.URL,
			client: &http.Client{
				Timeout: 5 * time.Second,
			}},
	}
	at.ServeHTTP(rec, req)
	require.Equal(t, 1, len(fakeCache.MockValues))
	if rec.Code != 200 {
		t.Fatalf("wrong code, got %d want %d", rec.Code, 200)
	}
	if body := rec.Body.String(); body != "stuff" {
		t.Errorf("Wrong body, got %q want %q", body, "stuff")
	}
}

func TestGetCachedToken(t *testing.T) {
	rec := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/someurl", nil)
	require.Nil(t, err)
	req.Header.Set("X-Auth-Token", "abcd")
	val := token{ExpiresAt: time.Now().Add(5 * time.Second), IssuedAt: time.Now()}
	fakeCache := mockTokenMemacacheRing{MockValues: map[string]interface{}{"abcd": val}}
	fakeContext := &ProxyContext{
		Logger:                 zap.NewNop(),
		ProxyContextMiddleware: &ProxyContextMiddleware{Cache: &fakeCache},
	}

	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", fakeContext))

	passthrough := checkHeaders(t, map[string]string{
		"X-Identity-Status": "Confirmed",
	})
	at := &authToken{
		next: passthrough,
	}
	at.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("wrong code, got %d want %d", rec.Code, 200)
	}
	if body := rec.Body.String(); body != "stuff" {
		t.Errorf("Wrong body, got %q want %q", body, "stuff")
	}
}

func TestWriteCachedToken(t *testing.T) {
	rec := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/someurl", nil)
	require.Nil(t, err)
	req.Header.Set("X-Auth-Token", "abcd")
	fakeCache := mockTokenMemacacheRing{MockValues: make(map[string]interface{})}
	fakeContext := &ProxyContext{
		Logger:                 zap.NewNop(),
		ProxyContextMiddleware: &ProxyContextMiddleware{Cache: &fakeCache},
	}
	req = req.WithContext(context.WithValue(req.Context(), "proxycontext", fakeContext))
	expectedExpiry := time.Now().Add(5 * time.Second).Round(time.Second)
	identityServ := fakeIdentityServer(201, 200, fmt.Sprintf(`
{
  "token": {
    "expires_at": "%s",
    "issued_at": "2015-10-08T15:09:11Z"
  }
}
	`, expectedExpiry.Format(time.RFC3339)))
	defer identityServ.Close()
	passthrough := checkHeaders(t, map[string]string{
		"X-Identity-Status": "Confirmed",
	})
	at := &authToken{
		next: passthrough,
		identity: &identity{authURL: identityServ.URL,
			client: &http.Client{
				Timeout: 5 * time.Second,
			}},
	}
	at.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("wrong code, got %d want %d", rec.Code, 200)
	}
	var tok token
	var tokint interface{}
	var ok bool
	if tokint, ok = fakeCache.MockValues["abcd"]; !ok {
		t.Fatal("token was not cached")
	}
	if tok, ok = tokint.(token); !ok {
		t.Fatal("token corrupt")
	}
	if !tok.ExpiresAt.Equal(expectedExpiry) {
		t.Fatalf("cached element has incorrect value. expected %q, got %q", expectedExpiry, tok.ExpiresAt)
	}

}
