package engine

import (
	"strings"
	"testing"
)

// chunkAll runs a whole token stream through a chunker and returns every
// chunk it produced, the way handleTurn does.
func chunkAll(c *speechChunker, tokens ...string) []string {
	var out []string
	for _, tok := range tokens {
		out = append(out, c.push(tok)...)
	}
	if rest := c.flush(); rest != "" {
		out = append(out, rest)
	}
	return out
}

func TestSpeechChunker(t *testing.T) {
	tests := []struct {
		name   string
		tokens []string
		want   []string
	}{{
		// The first chunk is cut as early as there is something to say: it
		// is the only one whose latency the caller can feel.
		name:   "first chunk goes out early, the rest are batched",
		tokens: []string{"Sure. ", "It rains ", "a lot there. ", "Bring a coat. ", "You'll want boots too."},
		want:   []string{"Sure.", " It rains a lot there. Bring a coat. You'll want boots too."},
	}, {
		// The failure this whole type exists to prevent: a reply arriving as
		// a shower of tiny fragments, each costing its own TTS round trip.
		name:   "short sentences are batched instead of one call each",
		tokens: []string{"Yes. ", "No. ", "Maybe. ", "I think so. ", "It depends. ", "Hard to say."},
		want:   []string{"Yes.", " No. Maybe. I think so. It depends. Hard to say."},
	}, {
		// "1." must not become a chunk of its own — it is a whole synthesis
		// call that produces nothing anyone wants to hear.
		name:   "punctuation-only fragments are absorbed, not synthesized",
		tokens: []string{"Here goes. ", "1. ", "Walk east until the river."},
		want:   []string{"Here goes.", " 1. Walk east until the river."},
	}, {
		// CJK has no spaces, so a chunk that trimmed both edges would run
		// its neighbours together — but a latin one must keep the space that
		// separates it from the previous sentence.
		name:   "CJK terminators split the same way",
		tokens: []string{"好的。", "今天天气不错，适合出门散步。", "你还想知道别的吗？"},
		want:   []string{"好的。", "今天天气不错，适合出门散步。你还想知道别的吗？"},
	}, {
		name:   "a reply with no terminator at all still comes out",
		tokens: []string{"just a fragment"},
		want:   []string{"just a fragment"},
	}, {
		// A reply that is one long sentence has its only full stop at the
		// very end. Letting the first chunk break at a comma is what stops
		// time-to-first-audio from waiting on the entire reply to generate.
		name:   "a one-sentence reply still starts speaking early",
		tokens: []string{"杭州是浙江省会，", "一座有着千年历史的江南名城，", "以西湖美景和龙井茶香闻名。"},
		want:   []string{"杭州是浙江省会，一座有着千年历史的江南名城，", "以西湖美景和龙井茶香闻名。"},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := chunkAll(newSpeechChunker(40, 180), tc.tokens...)
			if strings.Join(got, "|") != strings.Join(tc.want, "|") {
				t.Errorf("chunks =\n  %q\nwant\n  %q", got, tc.want)
			}
			// Whatever the split, no text may be invented or lost: the
			// chunks are what gets captioned, so they have to add back up to
			// the reply.
			if joined, full := strings.Join(got, ""), strings.TrimSpace(strings.Join(tc.tokens, "")); joined != full {
				t.Errorf("chunks rejoin to %q, want the whole reply %q", joined, full)
			}
		})
	}
}

// TestSpeechChunkerBreaksRunOnSentences proves MaxChunkRunes stops a model
// that never reaches a terminator from holding the entire reply back — it
// breaks at a clause boundary, which is still somewhere a listener expects a
// pause.
func TestSpeechChunkerBreaksRunOnSentences(t *testing.T) {
	const maxRunes = 30
	long := "one, two, three, four, five, six, seven, eight, nine, ten, eleven, twelve"
	got := chunkAll(newSpeechChunker(40, maxRunes), long)
	if len(got) < 2 {
		t.Fatalf("chunks = %q, want the run-on sentence broken into several", got)
	}
	for _, c := range got {
		if n := len([]rune(c)); n > maxRunes {
			t.Errorf("chunk %q is %d runes, want no more than the %d-rune cap", c, n, maxRunes)
		}
	}
	if joined := strings.Join(got, ""); joined != long {
		t.Errorf("chunks rejoin to %q, want %q", joined, long)
	}
}
