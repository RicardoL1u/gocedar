package gocedar

import (
	"log"
	"testing"

	"github.com/vcaesar/tt"
)

var (
	cd      *Cedar
	useMMap = true
	words   = []string{
		"魔术师",
		"Cryin'",
		"荒谬世界",
		"活得精彩",
		"梦",
		"声音",
		"最后的答案",
		"阿博",
		"夜长梦多",
		"爱上你只是我的错",
		"毕业",
		"十八岁",
		"比我勇敢",
		"寂寞电梯",
		"你又不是我	",
	}
)

func TestFLoadData(t *testing.T) {
	cd := New(&Options{
		Reduced:  true,
		UseMMap:  useMMap,
		MMapPath: dumpPath,
	})
	defer cd.Close()
	// add the words
	for i, word := range words {
		err := cd.Insert([]byte(word), i)
		tt.Nil(t, err)
	}
	val, err := cd.Get([]byte("魔术师"))
	tt.Nil(t, err)
	tt.Equal(t, 0, val)

	// update the words
	for i, word := range words {
		err := cd.Delete([]byte(word))
		tt.Nil(t, err, word)

		err = cd.Update([]byte(word), i)
		tt.Nil(t, err)
	}

	// delete not used word
	for i := 10; i < 15; i++ {
		err := cd.Delete([]byte(words[i]))
		tt.Nil(t, err)
	}
}

func TestFFind(t *testing.T) {
	if !useMMap {
		return
	}
	cd := New(&Options{
		Reduced:  true,
		UseMMap:  useMMap,
		MMapPath: dumpPath,
	})
	defer cd.Close()
	key, err := cd.Find([]byte("最后的答案"), 0)
	tt.Nil(t, err)
	log.Printf("key %d\n", key)
	// tt.Equal(t, 0, key)

	val, err := cd.Get([]byte("魔术师"))
	tt.Nil(t, err)
	tt.Equal(t, 0, val)

	// to, err := cd.Jump([]byte("活得精彩"), 0)
	// tt.Nil(t, err)
	// log.Printf("to %d\n", to)
	// // tt.Equal(t, 352, to)
	// val, err = cd.Value(to)
	// tt.Nil(t, err)
	// // tt.Equal(t, 3, val)
}
