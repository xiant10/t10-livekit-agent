package engine

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// isSentenceEnd reports whether r terminates a speakable chunk.
func isSentenceEnd(r rune) bool {
	switch r {
	case '。', '！', '!', '？', '?', '.', '\n':
		return true
	}
	return false
}

// isClauseBreak reports whether r is a natural place to break a chunk that
// has run past maxRunes without ever reaching a sentence terminator (a
// model emitting one long comma-spliced sentence, a list on a single line).
// Breaking here still lands on a pause a listener expects. Whitespace is
// deliberately not on the list: a cut in the middle of a phrase is worse
// than no cut at all, and a run of text with no punctuation anywhere in it
// is better spoken whole.
func isClauseBreak(r rune) bool {
	switch r {
	case '，', ',', '；', ';', '：', ':', '、':
		return true
	}
	return false
}

// speakableRunes counts the letters and digits in text — what is actually
// left to pronounce once punctuation and whitespace are set aside.
func speakableRunes(text string) int {
	n := 0
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			n++
		}
	}
	return n
}

func hasSpeakableContent(text string) bool { return speakableRunes(text) > 0 }

// minSpeakableRunes is how much a chunk has to be *worth* before it earns
// its own synthesis call. Two is the useful line: it keeps "OK." and "好的。"
// — real, complete answers someone should hear immediately — while refusing
// to spend a whole provider round trip on the "1." that a numbered list
// starts with.
const minSpeakableRunes = 2

// firstChunkClauseRunes lets the *first* chunk of a reply — and only the
// first — break at a comma rather than holding out for a full stop.
//
// A reply that is one long sentence has exactly one sentence terminator, at
// the very end, so without this the first chunk cannot be cut until the
// model has finished generating the whole thing. Measured on a Chinese reply
// of this shape, that pushed time-to-first-audio out by about three seconds
// for no reason: the opening clause was ready to speak long before the rest
// existed. Twelve runes is enough that the chunk isn't a clipped "Well,"
// while still catching the first comma of a normal opening clause — at
// sixteen, a measured reply missed its only early break by one rune and
// waited for the full stop anyway.
const firstChunkClauseRunes = 12

// speechChunker slices an LLM token stream into the units that actually get
// synthesized. It exists because "one TTS call per sentence" — what this
// used to do — is the wrong granularity in both directions:
//
//   - Too small. Every Synthesize call is its own provider WebSocket
//     handshake plus session setup, ~1s of fixed cost regardless of length.
//     A reply chopped into eighteen sentences paid that eighteen times, and
//     since those calls all ran at once they queued behind each other at the
//     provider: measured on a real call, three-byte chunks were taking 7.7s
//     and one hit the 20s idle timeout outright. Batching short sentences up
//     to MinChunkRunes collapses that into a handful of calls.
//   - Too large, for the *first* chunk. Waiting for MinChunkRunes before
//     saying anything would add seconds to the one number a caller actually
//     feels — time to first audio. So the first chunk of every reply is cut
//     at the first sentence boundary with words in it, and only the ones
//     after it are batched.
//
// MaxChunkRunes bounds the other end: a model that emits one long
// comma-spliced sentence would otherwise never hit a terminator and would be
// held until the reply ended.
type speechChunker struct {
	minRunes int
	maxRunes int

	buf     strings.Builder
	emitted int
}

func newSpeechChunker(minRunes, maxRunes int) *speechChunker {
	return &speechChunker{minRunes: minRunes, maxRunes: maxRunes}
}

// push appends a token and returns whatever complete chunks that made
// available, in order (usually none).
func (c *speechChunker) push(delta string) []string {
	c.buf.WriteString(delta)
	var out []string
	for {
		chunk, ok := c.take()
		if !ok {
			break
		}
		if chunk != "" {
			out = append(out, chunk)
		}
	}
	return out
}

// flush returns whatever is left once the token stream has ended.
func (c *speechChunker) flush() string {
	rest := c.trim(c.buf.String())
	c.buf.Reset()
	if rest != "" {
		c.emitted++
	}
	return rest
}

// trim tidies a chunk's edges. Trailing whitespace (usually the newline the
// chunk was cut on) always goes; leading whitespace is kept on every chunk
// but the first, because the space separating two sentences lives at the
// *start* of the second one — dropping it would run the reply's captions
// together as "Hello.How are you?".
func (c *speechChunker) trim(chunk string) string {
	const space = " \t\r\n"
	if c.emitted == 0 {
		chunk = strings.TrimLeft(chunk, space)
	}
	return strings.TrimRight(chunk, space)
}

// take pulls the next complete chunk off the buffer, or reports false if the
// buffer doesn't hold one yet.
func (c *speechChunker) take() (string, bool) {
	buf := c.buf.String()

	// Every chunk but the first has to reach minRunes before it may be cut;
	// the first one has no length floor at all, because it alone decides how
	// long the caller waits to hear anything.
	min := 1
	if c.emitted > 0 {
		min = c.minRunes
	}

	// Cut at the first sentence terminator that leaves a chunk of at least
	// min runes *and* enough to be worth saying — terminators before that,
	// and ones that would only yield punctuation, get absorbed into this
	// chunk rather than costing their own TTS round trip.
	n := 0
	lastBreak := -1
	for i, r := range buf {
		n++
		if isClauseBreak(r) && n <= c.maxRunes {
			lastBreak = i + utf8.RuneLen(r)
		}
		cuttable := (n >= min && isSentenceEnd(r)) ||
			(c.emitted == 0 && n >= firstChunkClauseRunes && isClauseBreak(r))
		if cuttable {
			if end := i + utf8.RuneLen(r); speakableRunes(buf[:end]) >= minSpeakableRunes {
				return c.cut(buf, end), true
			}
		}
	}

	// No usable terminator, and the buffer has run long: break at the last
	// clause boundary within the cap instead. Failing even that, don't break
	// at all — see isClauseBreak.
	if n >= c.maxRunes && lastBreak > 0 && speakableRunes(buf[:lastBreak]) >= minSpeakableRunes {
		return c.cut(buf, lastBreak), true
	}
	return "", false
}

// cut splits buf at byte offset end, keeping the remainder buffered.
func (c *speechChunker) cut(buf string, end int) string {
	chunk := c.trim(buf[:end])
	c.buf.Reset()
	c.buf.WriteString(buf[end:])
	c.emitted++
	return chunk
}
