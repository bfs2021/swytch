package stream

import (
	"context"
	"strings"
	"testing"

	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

type keyReadFailure struct {
	key string
}

func (r keyReadFailure) MayHold(key string) bool { return key == r.key }
func (keyReadFailure) MarkPending(string)        {}
func (r keyReadFailure) CloudTips(ctx context.Context, key string) ([]effects.Tip, []effects.CloudEffect, []effects.Tip, error) {
	if key == r.key {
		return failingCloudReader{}.CloudTips(ctx, key)
	}

	return nil, nil, nil, nil
}

func TestStreamMetadataReadErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler shared.HandlerFunc
		args    []string
		key     string
		group   bool
	}{
		{"group registry info", handleXInfo, []string{"GROUPS", "s"}, "s\x01g\x01__registry__", false},
		{"group registry trim", handleXTrim, []string{"s", "MAXLEN", "0", "ACKED"}, "s\x01g\x01__registry__", false},
		{"group registry delete", handleXDelEx, []string{"s", "DELREF", "IDS", "1", "1-0"}, "s\x01g\x01__registry__", false},
		{"group create", handleXGroup, []string{"CREATE", "s", "g", "0"}, groupKey("s", "g"), false},
		{"group destroy", handleXGroup, []string{"DESTROY", "s", "g"}, groupKey("s", "g"), false},
		{"group consumer", handleXGroup, []string{"CREATECONSUMER", "s", "g", "c"}, groupKey("s", "g"), false},
		{"group ack", handleXAck, []string{"s", "g", "1-0"}, groupKey("s", "g"), false},
		{"group pending", handleXPending, []string{"s", "g"}, groupKey("s", "g"), false},
		{"group info", handleXInfo, []string{"GROUPS", "s"}, groupKey("s", "g"), false},
		{"group full info", handleXInfo, []string{"STREAM", "s", "FULL"}, groupKey("s", "g"), false},
		{"pel info", handleXInfo, []string{"GROUPS", "s"}, pelKey("s", "g"), true},
		{"pel full info", handleXInfo, []string{"STREAM", "s", "FULL"}, pelKey("s", "g"), true},
		{"pel consumers", handleXInfo, []string{"CONSUMERS", "s", "g"}, pelKey("s", "g"), true},
		{"pel ack", handleXAck, []string{"s", "g", "1-0"}, pelKey("s", "g"), true},
		{"pel pending", handleXPending, []string{"s", "g"}, pelKey("s", "g"), true},
		{"config info", handleXInfo, []string{"STREAM", "s"}, cfgKey("s"), false},
		{"config append", handleXAdd, []string{"s", "*", "f", "v"}, cfgKey("s"), false},
		{"config idempotent append", handleXAdd, []string{"s", "IDMP", "p", "iid", "*", "f", "v"}, cfgKey("s"), false},
		{"config update", handleXCfgSet, []string{"s", "IDMP-DURATION", "50"}, cfgKey("s"), false},
		{"producer registry", handleXInfo, []string{"STREAM", "s"}, "s\x01p\x01__registry__", false},
		{"producer info", handleXInfo, []string{"STREAM", "s"}, producerKey("s", "p"), false},
		{"producer dedup", handleXAdd, []string{"s", "IDMP", "p", "iid", "*", "f", "v"}, producerKey("s", "p"), false},
		{"producer record", handleXIdmpRecord, []string{"s", "p", "iid", "1-0"}, producerKey("s", "p"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestEnv()
			cmd := streamTestCommand(env)
			// Seed only the stream data. Missing auxiliary keys must remain
			// distinguishable from auxiliary keys that cannot be read.
			if err := emitStreamInsert(cmd, "s", shared.StreamID{Ms: 1}, [][]byte{[]byte("f"), []byte("v")}); err != nil {
				t.Fatal(err)
			}

			if tc.key == groupKey("s", "g") || tc.group {
				if err := emitGroupRegistryInsert(cmd, "s", "g"); err != nil {
					t.Fatal(err)
				}
			}

			if tc.group {
				if err := emitGroupInsert(cmd, "s", "g", encodeGroupMeta(shared.StreamID{}, 0, 0)); err != nil {
					t.Fatal(err)
				}
			}

			if tc.key == producerKey("s", "p") {
				if err := emitProducerRegistryInsert(cmd, "s", "p"); err != nil {
					t.Fatal(err)
				}
			}

			if err := env.ctx.Flush(); err != nil {
				t.Fatal(err)
			}

			env.eng.SetCloudReader(keyReadFailure{key: tc.key})
			runWithoutFlush(t, env, tc.handler, tc.args...)

			got := env.buf.String()
			if !strings.HasPrefix(got, "-cloud unavailable:") || strings.Count(got, "\r\n") != 1 {
				t.Fatalf("want one complete storage error, got %q", got)
			}
		})
	}
}
