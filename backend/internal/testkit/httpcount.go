package testkit

import (
	"net"
	"net/http"
	"sync/atomic"
)

// HTTPCount 安装到 httptest.Server.Config.ConnState，并包装其
// handler，以统计实际接受的连接和已解析的 HTTP 请求。
type HTTPCount struct{ connections, requests atomic.Int64 }

func (c *HTTPCount) ConnState(_ net.Conn, state http.ConnState) {
	if state == http.StateNew {
		c.connections.Add(1)
	}
}
func (c *HTTPCount) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { c.requests.Add(1); next.ServeHTTP(w, r) })
}
func (c *HTTPCount) Counts() (connections, requests int64) {
	return c.connections.Load(), c.requests.Load()
}
