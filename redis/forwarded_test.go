package redis

import (
	"strings"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
)

func forwardedCommands(commands ...[]string) *pb.ForwardedTransaction {
	tx := &pb.ForwardedTransaction{}

	for _, args := range commands {
		cmd := &pb.ForwardedCommand{CommandName: []byte(args[0])}
		for _, arg := range args[1:] {
			cmd.Args = append(cmd.Args, []byte(arg))
		}

		tx.Commands = append(tx.Commands, cmd)
	}

	return tx
}

func TestForwardedTransactionReplies(t *testing.T) {
	h := newSSIHandler(t)
	resp := h.HandleForwardedTransaction(forwardedCommands(
		[]string{"SET", "key", "value"},
		[]string{"GET", "key"},
		[]string{"LPUSH", "key", "item"},
	))

	want := "*3\r\n+OK\r\n$5\r\nvalue\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	if resp.GetError() || string(resp.GetRespData()) != want {
		t.Fatalf("forwarded response: %v", resp)
	}

	ssiWant(t, h, ssiConn(h), "$5\r\nvalue\r\n", "GET", "key")
}

func TestForwardedTransactionFlushFailure(t *testing.T) {
	h := newSSIHandler(t)
	h.engine.SetCloudReader(delFailingCloudReader{key: "unavailable"})
	resp := h.HandleForwardedTransaction(forwardedCommands(
		[]string{"SET", "before", "value"},
		[]string{"GET", "unavailable"},
	))
	h.engine.SetCloudReader(nil)

	if resp.GetError() || !strings.HasPrefix(string(resp.GetRespData()), "-TRYAGAIN ") || strings.Count(string(resp.GetRespData()), "\r\n") != 1 {
		t.Fatalf("flush failure must replace all provisional replies: %v", resp)
	}

	ssiWant(t, h, ssiConn(h), "$-1\r\n", "GET", "before")
}

func TestForwardedTransactionValidationFailure(t *testing.T) {
	h := newSSIHandler(t)

	resp := h.HandleForwardedTransaction(forwardedCommands(
		[]string{"SET", "before", "value"},
		[]string{"SET"},
	))
	if !strings.HasPrefix(string(resp.GetRespData()), "-EXECABORT ") {
		t.Fatalf("expected EXECABORT: %v", resp)
	}

	ssiWant(t, h, ssiConn(h), "$-1\r\n", "GET", "before")
}
