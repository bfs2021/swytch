package redis

import (
	"strings"
	"testing"

	"github.com/swytchdb/swytch/redis/shared"
)

func TestXGroupInvalidIDTransaction(t *testing.T) {
	for _, ssi := range []bool{false, true} {
		h := newSSIHandler(t)
		h.ssi = ssi
		run := commandClient(h)
		run("MULTI")
		run("SET", "before", "value")

		reply := run("XGROUP", "CREATE", "s", "g", "invalid-id", "MKSTREAM")

		wantError := "-ERR Invalid stream ID specified as stream command argument\r\n"
		if ssi {
			if reply != wantError {
				t.Fatal(reply)
			}
		} else if reply != "+QUEUED\r\n" {
			t.Fatal(reply)
		}

		if got := run("EXEC"); got != "*2\r\n+OK\r\n"+wantError {
			t.Fatalf("invalid ID should fail only its EXEC entry: %q", got)
		}

		if got := run("GET", "before"); got != "$5\r\nvalue\r\n" {
			t.Fatal(got)
		}

		if got := run("EXISTS", "s"); got != ":0\r\n" {
			t.Fatalf("invalid ID created a stream: %q", got)
		}
	}
}

func TestStreamReadFailureRollback(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		key  string
	}{
		{"XGROUP", []string{"XGROUP", "CREATE", "new", "g", "0", "MKSTREAM"}, "new\x01g\x01g"},
		{"XTRIM", []string{"XTRIM", "s", "MAXLEN", "0", "DELREF"}, "s\x01g\x01__registry__"},
		{"XDELEX", []string{"XDELEX", "s", "DELREF", "IDS", "1", "1-0"}, "s\x01g\x01__registry__"},
		{"XADD", []string{"XADD", "s", "IDMP", "p", "iid", "*", "f", "v"}, "s\x01p\x01p"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"standalone", "queued", "ssi"} {
				t.Run(mode, func(t *testing.T) {
					h := newSSIHandler(t)
					h.ssi = mode == "ssi"

					run := commandClient(h)
					if got := run("XADD", "s", "1-0", "f", "v"); got != "$3\r\n1-0\r\n" {
						t.Fatal(got)
					}

					withCommandFault(h, shared.ParseCommandType([]byte(tc.name)), func(_ *shared.Command) func() {
						h.engine.SetCloudReader(delFailingCloudReader{key: tc.key})

						return func() { h.engine.SetCloudReader(nil) }
					})

					if mode != "standalone" {
						run("MULTI")
					}

					run("SET", "before", "value")
					reply := run(tc.args...)
					run("SET", "after", "value")

					if mode == "queued" {
						if reply != "+QUEUED\r\n" {
							t.Fatal(reply)
						}

						reply = run("EXEC")
						if !strings.HasPrefix(reply, "*3\r\n+OK\r\n-cloud unavailable:") || !strings.HasSuffix(reply, "\r\n+OK\r\n") {
							t.Fatalf("EXEC lost command replies: %q", reply)
						}
					} else {
						if !strings.HasPrefix(reply, "-cloud unavailable:") || strings.Count(reply, "\r\n") != 1 {
							t.Fatalf("want one storage error, got %q", reply)
						}

						if mode == "ssi" {
							if got := run("EXEC"); got != "*3\r\n+OK\r\n"+reply+"+OK\r\n" {
								t.Fatal(got)
							}
						}
					}

					if got := run("XLEN", "s"); got != ":1\r\n" {
						t.Fatalf("failed command changed stream: %q", got)
					}

					if got := run("EXISTS", "new"); got != ":0\r\n" {
						t.Fatalf("failed command created a stream: %q", got)
					}

					for _, key := range []string{"before", "after"} {
						if got := run("GET", key); got != "$5\r\nvalue\r\n" {
							t.Fatalf("lost surrounding write: %q", got)
						}
					}
				})
			}
		})
	}
}
