package redis

import "testing"

func TestHGetExPersistsFieldExpiry(t *testing.T) {
	h := newSSIHandler(t)
	run := commandClient(h)
	run("HSET", "h", "a", "one", "b", "two")

	wantValues := "*2\r\n$3\r\none\r\n$3\r\ntwo\r\n"
	if got := run("HGETEX", "h", "EXAT", "4102444800", "FIELDS", "2", "a", "b"); got != wantValues {
		t.Fatal(got)
	}

	if got := run("HEXPIRETIME", "h", "FIELDS", "2", "a", "b"); got != "*2\r\n:4102444800\r\n:4102444800\r\n" {
		t.Fatalf("HGETEX expiry was not committed: %q", got)
	}

	if got := run("HGETEX", "h", "PERSIST", "FIELDS", "2", "a", "b"); got != wantValues {
		t.Fatal(got)
	}

	if got := run("HPTTL", "h", "FIELDS", "2", "a", "b"); got != "*2\r\n:-1\r\n:-1\r\n" {
		t.Fatalf("HGETEX PERSIST was not committed: %q", got)
	}
}
