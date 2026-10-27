package redis

import (
	"strings"
	"testing"
)

func TestSetStoreDestinationReadFailure(t *testing.T) {
	for _, name := range []string{"SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE"} {
		t.Run(name, func(t *testing.T) {
			for _, ssi := range []bool{false, true} {
				h := newSSIHandler(t)
				run := commandClient(h)
				run("SADD", "source", "member")

				if ssi {
					run("MULTI")
					run("SET", "before", "value")
				}

				h.engine.SetCloudReader(delFailingCloudReader{key: "destination"})

				reply := run(name, "destination", "source")

				h.engine.SetCloudReader(nil)

				if !strings.HasPrefix(reply, "-cloud unavailable:") {
					t.Fatalf("uncertain destination treated as absent: %q", reply)
				}

				if ssi {
					if got := run("EXEC"); got != "*2\r\n+OK\r\n"+reply {
						t.Fatal(got)
					}

					if got := run("GET", "before"); got != "$5\r\nvalue\r\n" {
						t.Fatalf("lost earlier transaction write: %q", got)
					}
				}

				if got := run("EXISTS", "destination"); got != ":0\r\n" {
					t.Fatalf("failed store created destination: %q", got)
				}
			}
		})
	}
}
