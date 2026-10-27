package shared

import (
	"bytes"
	"testing"
)

func TestDiscardPartialErrorReply(t *testing.T) {
	for _, write := range []func(*Writer){
		func(w *Writer) { w.WriteError("ERR failure") },
		func(w *Writer) { w.WriteErrorf("failure") },
		func(w *Writer) { w.WriteBlobError("ERR", "failure") },
		func(w *Writer) { w.WriteWrongType() },
		func(w *Writer) { w.WriteNoAuth() },
		func(w *Writer) { w.WriteWrongNumArguments("test") },
		func(w *Writer) { w.WriteWrongNumArgs("test") },
		func(w *Writer) { w.WriteUnknownCommand("test", nil) },
		func(w *Writer) { w.WriteSyntaxError() },
		func(w *Writer) { w.WriteNotInteger() },
		func(w *Writer) { w.WriteOutOfRange() },
	} {
		for _, protocol := range []ProtocolVersion{RESP2, RESP3} {
			var buf, expected bytes.Buffer

			w, errorWriter := NewWriter(&buf), NewWriter(&expected)
			w.SetProtocol(protocol)
			errorWriter.SetProtocol(protocol)
			write(errorWriter)
			w.WriteArray(2)
			w.WriteOK()

			prefix := buf.String()
			start, count := buf.Len(), w.ErrorCount()
			w.WriteArray(3)
			w.WriteBulkStringStr("partial")
			write(w)
			w.WriteInteger(1)

			if !w.DiscardPartialReply(start, count) || buf.String() != prefix+expected.String() {
				t.Fatalf("protocol %v: got %q, want %q", protocol, buf.String(), prefix+expected.String())
			}

			w.ResetErrorFlag()
			start, count = buf.Len(), w.ErrorCount()
			w.WriteBulkStringStr("-ERR this is a value, not an error")

			if w.DiscardPartialReply(start, count) {
				t.Fatal("stale error affected next command")
			}
		}
	}
}
