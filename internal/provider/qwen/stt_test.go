package qwen

import (
	"reflect"
	"testing"
)

func TestStreamingPreviewIncludesStashAndReplacesCorrections(t *testing.T) {
	events := make(chan realtimeEvent, 8)
	for _, ev := range []realtimeEvent{
		{"type": "conversation.item.input_audio_transcription.text", "text": "", "stash": "北京"},
		{"type": "conversation.item.input_audio_transcription.text", "text": "", "stash": "背景"},
		{"type": "conversation.item.input_audio_transcription.text", "text": "背景", "stash": "音乐"},
		{"type": "conversation.item.input_audio_transcription.text", "text": "背景", "stash": "音乐"},
		{"type": "conversation.item.input_audio_transcription.completed", "transcript": "背景音乐。"},
	} {
		events <- ev
	}
	close(events)
	var snapshots []string
	s := &stream{client: &realtimeClient{events: events}, resultCh: make(chan sttResult, 1), onInterim: func(text string) { snapshots = append(snapshots, text) }}
	s.runEventConsumer()
	if !reflect.DeepEqual(snapshots, []string{"北京", "背景", "背景音乐"}) {
		t.Fatalf("previews = %q", snapshots)
	}
	if got := <-s.resultCh; got.transcript != "背景音乐。" || got.err != nil {
		t.Fatalf("final = %+v", got)
	}
}

func TestPreviewSupportsBothSnapshotAndLiteralDeltaEvents(t *testing.T) {
	var preview string
	for _, ev := range []realtimeEvent{{"delta": "hello "}, {"delta": "world"}, {"text": "Hello ", "stash": "world!"}} {
		updatePreview(&preview, ev)
	}
	if preview != "Hello world!" {
		t.Fatalf("preview = %q", preview)
	}
}
