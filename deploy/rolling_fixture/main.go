// Command rolling_fixture is a network-isolated handover test backend.
package main

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func main() {
	name := os.Getenv("FIXTURE_NAME")
	var mu sync.Mutex
	counts := map[string]int{}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/counts" {
			mu.Lock()
			defer mu.Unlock()
			_ = json.NewEncoder(w).Encode(counts)
			return
		}
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
		id := r.URL.Query().Get("id")
		mu.Lock()
		counts[id]++
		mu.Unlock()
		delay, _ := strconv.Atoi(r.URL.Query().Get("delay"))
		if delay < 0 || delay > 15 {
			http.Error(w, "invalid fixture delay", 400)
			return
		}
		if r.URL.Path == "/ws" || r.URL.Path == "/eof" {
			conn, buf, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			if r.URL.Path == "/eof" {
				return
			}
			sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			fmt.Fprintf(buf, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:]))
			_ = buf.Flush()
			for i := 0; i < 5; i++ {
				data := fmt.Sprintf("%s:%d", name, i)
				_, _ = conn.Write(append([]byte{0x81, byte(len(data))}, data...))
				time.Sleep(time.Second)
			}
			_, _ = conn.Write([]byte{0x88, 0})
			return
		}
		if r.URL.Path == "/stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			for i := 0; i < 5; i++ {
				fmt.Fprintf(w, "data: %s:%d\n\n", name, i)
				w.(http.Flusher).Flush()
				time.Sleep(time.Second)
			}
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		time.Sleep(time.Duration(delay) * time.Second)
		if r.URL.Path == "/503" {
			w.WriteHeader(503)
			fmt.Fprint(w, `{"error":{"code":"fixture_unavailable"}}`)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"backend": name, "body": string(body), "id": id})
	})
	srv := &http.Server{Addr: ":8181", Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
