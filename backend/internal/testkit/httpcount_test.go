package testkit

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPCountTracksReuse(t *testing.T) {
	var counts HTTPCount
	s := httptest.NewUnstartedServer(counts.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") })))
	s.Config.ConnState = counts.ConnState
	s.Start()
	defer s.Close()
	client := s.Client()
	for range 2 {
		resp, err := client.Get(s.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	conn, requests := counts.Counts()
	if conn != 1 || requests != 2 {
		t.Fatalf("连接数=%d 请求数=%d", conn, requests)
	}
}
