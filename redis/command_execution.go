package redis

import (
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// runDataCommand rolls back a failed data-write command, including its partial
// reply. It does not roll back an enclosing transaction or an entire script:
// Redis scripts retain successful earlier commands when a later command fails.
func runDataCommand(entry *shared.CommandEntry, cmd *shared.Command, w *shared.Writer, run shared.CommandRunner) {
	if entry.Flags&shared.FlagWrite == 0 || entry.Keys == nil || isScriptCommand(cmd.Type) {
		run()

		return
	}

	var savepoint *effects.ContextSavepoint
	if cmd.Context != nil {
		savepoint = cmd.Context.TakeSavepoint()
	}

	start, errorsBefore := w.Buffer().Len(), w.ErrorCount()

	run()

	if w.DiscardPartialReply(start, errorsBefore) && cmd.Context != nil {
		cmd.Context.RestoreSavepoint(savepoint)
	}
}

func isScriptCommand(cmd shared.CommandType) bool {
	//exhaustive:ignore
	switch cmd {
	case shared.CmdEval, shared.CmdEvalSha, shared.CmdEvalRO, shared.CmdEvalShaRO,
		shared.CmdFcall, shared.CmdFcallRO:
		return true
	default:
		return false
	}
}
