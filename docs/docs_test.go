package docs

import (
	"testing"

	"github.com/xiant10/t10-modkit/docsmcp"
)

// The image serves these, so broken front matter must fail here, not at startup.
func TestServable(t *testing.T) {
	h, err := docsmcp.New(docsmcp.Options{Component: "t10-livekit-agent", Docs: FS})
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Docs()) == 0 {
		t.Error("no doc is for people building on t10")
	}
}
