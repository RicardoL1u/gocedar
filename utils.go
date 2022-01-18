package gocedar

import (
	"bytes"

	"github.com/go-ego/gse"
)

func textSliceToBytes(text []gse.Text) []byte {
	var buf bytes.Buffer
	for _, word := range text {
		buf.Write(word)
	}

	return buf.Bytes()
}
