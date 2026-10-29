package roomprotocol

// Topic constants define the LiveKit text-stream and byte-stream topics
// this agent uses. All topics share one LiveKit room per conversation.
// See docs/DESIGN.md §4 for the full protocol.
const (
	// TopicChat carries streamed text replies (agent → client) and text
	// messages (client → agent).
	TopicChat = "agent.chat"

	// TopicVoice carries uploaded WAV recordings (client → agent) and
	// synthesized WAV replies (agent → client) as byte-streams.
	TopicVoice = "agent.voice"

	// TopicTranscript carries the agent's final, authoritative transcript
	// of what it heard — one message per utterance. Used by both voice-message
	// mode (before the LLM reply) and live-call mode (as each utterance is
	// committed).
	TopicTranscript = "agent.transcript"

	// TopicTranscriptInterim carries replaceable STT hypotheses, live-call
	// mode only. One stream per utterance carries snapshot-jsonl updates;
	// the later TopicTranscript message is authoritative.
	TopicTranscriptInterim = "agent.transcript.interim"

	// TopicState carries call-state transitions (listening/thinking/speaking/
	// interrupted/idle) — live-call mode only. The "interrupted" message is
	// load-bearing: it tells the client to drop audio already in its speaker
	// buffer.
	TopicState = "agent.state"

	// Official LiveKit standard topics for agent interoperability:
	TopicLKChat          = "lk.chat"
	TopicLKTranscription = "lk.transcription"
)

// Stream and participant attributes are part of the room wire contract, not SDK details.
const (
	AttrEventOrder      = "event_order"
	AttrUtteranceID     = "utterance_id"
	AttrFormat          = "format"
	FormatSnapshotJSONL = "snapshot-jsonl"

	// LiveKit standard participant attributes for agent state persistence across reconnects:
	AttrLKAgentState         = "lk.agent.state"
	AttrLKTranscribedTrackID = "lk.transcribed_track_id"
	AttrLKSegmentID          = "lk.segment_id"
	AttrLKTranscriptionFinal = "lk.transcription_final"

	// Official agent state constants (matching lk.agent.state conventions):
	AgentStateInitializing = "initializing"
	AgentStateListening    = "listening"
	AgentStateThinking     = "thinking"
	AgentStateSpeaking     = "speaking"
	AgentStateIdle         = "idle"
)

// Message status is separate from live-call floor state. One ordered JSONL
// stream per request ends with done or error; audio_chunks counts WAV streams.
const (
	TopicMessageStatus = "agent.message.status"
	AttrRequestID      = "request_id"
	AttrChunkIndex     = "chunk_index"
)

type MessageStatus struct {
	State       string `json:"state"`
	Message     string `json:"message,omitempty"`
	AudioChunks int    `json:"audio_chunks,omitempty"`
}
