package redis

import (
	"strings"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

func TestScriptRollsBackOnlyFailedDataCommand(t *testing.T) {
	h := newSSIHandler(t)
	run := commandClient(h)
	run("SET", "source", "original")

	withCommandFault(h, shared.CmdMSet, func(_ *shared.Command) func() {
		h.engine.SetBroadcaster(&commandPartition{})

		return func() { h.engine.SetBroadcaster(nil) }
	})

	reply := run("EVAL", "redis.call('SET', KEYS[1], 'keep'); redis.call('MSET', KEYS[2], 'new', KEYS[3], 'new')", "3", "before", "source", "dest")
	if !strings.HasPrefix(reply, "-") {
		t.Fatalf("script should fail: %q", reply)
	}

	if got := run("GET", "before"); got != "$4\r\nkeep\r\n" {
		t.Fatalf("script lost its successful command: %q", got)
	}

	if got := run("GET", "source"); got != "$8\r\noriginal\r\n" {
		t.Fatalf("failed nested MSET retained its earlier write: %q", got)
	}

	if got := run("EXISTS", "dest"); got != ":0\r\n" {
		t.Fatalf("failed nested MSET created its destination: %q", got)
	}
}

func TestCompoundDataWriteRollback(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed [][]string
		args []string
		read []string
	}{
		{"list pop", [][]string{{"RPUSH", "target", "a", "b", "c"}}, []string{"LPOP", "target", "2"}, []string{"LRANGE", "target", "0", "-1"}},
		{"set pop", [][]string{{"SADD", "target", "a", "b", "c"}}, []string{"SPOP", "target", "2"}, []string{"SCARD", "target"}},
		{"sorted set pop", [][]string{{"ZADD", "target", "1", "a", "2", "b", "3", "c"}}, []string{"ZPOPMIN", "target", "2"}, []string{"ZRANGE", "target", "0", "-1", "WITHSCORES"}},
		{"set replacement", [][]string{{"SADD", "target", "original"}, {"SADD", "source", "new"}}, []string{"SUNIONSTORE", "target", "source"}, []string{"SMEMBERS", "target"}},
		{"sorted set replacement", [][]string{{"ZADD", "target", "1", "original"}, {"ZADD", "source", "2", "new"}}, []string{"ZUNIONSTORE", "target", "1", "source"}, []string{"ZRANGE", "target", "0", "-1", "WITHSCORES"}},
		{"sort replacement", [][]string{{"RPUSH", "target", "original"}, {"RPUSH", "source", "2", "1"}}, []string{"SORT", "source", "STORE", "target"}, []string{"LRANGE", "target", "0", "-1"}},
		{"value and expiry", [][]string{{"SET", "target", "original"}}, []string{"SET", "target", "new", "EX", "600"}, []string{"GET", "target"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newSSIHandler(t)

			run := commandClient(h)
			for _, seed := range tc.seed {
				if got := run(seed...); strings.HasPrefix(got, "-") {
					t.Fatal(got)
				}
			}

			original := run(tc.read...)
			run("MULTI")
			run("SET", "before", "keep")

			withCommandFault(h, shared.ParseCommandType([]byte(tc.args[0])), func(cmd *shared.Command) func() {
				h.engine.SetCloudReader(delFailingCloudReader{key: "unavailable"})

				injected := false

				h.engine.SetOnLocalEffect(func(_ effects.Tip, eff *pb.Effect) {
					if !injected && string(eff.GetKey()) == "target" && (eff.GetData() != nil || eff.GetMeta() != nil) {
						injected = true
						_, _, _ = cmd.Context.GetSnapshot("unavailable")
					}
				})

				return func() {
					h.engine.SetOnLocalEffect(nil)
					h.engine.SetCloudReader(nil)

					if !injected {
						t.Error("fault did not run after a write")
					}
				}
			})

			reply := run(tc.args...)
			if !strings.HasPrefix(reply, "-cloud unavailable:") || strings.Count(reply, "\r\n") != 1 {
				t.Fatalf("want only the write error, got %q", reply)
			}

			if got := run("EXEC"); got != "*2\r\n+OK\r\n"+reply {
				t.Fatalf("failed command damaged transaction: %q", got)
			}

			if got := run(tc.read...); got != original {
				t.Fatalf("failed command changed target: got %q, want %q", got, original)
			}

			if got := run("PTTL", "target"); got != ":-1\r\n" {
				t.Fatalf("failed command changed expiry: %q", got)
			}

			if got := run("GET", "before"); got != "$4\r\nkeep\r\n" {
				t.Fatalf("failed command lost earlier write: %q", got)
			}
		})
	}
}
