package cedar

import (
	"testing"

	"github.com/vcaesar/tt"
)

var (
	cd *Cedar

	words = []string{
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

func TestLoadData(t *testing.T) {
	cd := New(&Options{
		Reduced: true,
		UseMMap: true,
	})
	defer cd.Close()
	// add the words
	for i, word := range words {
		err := cd.Insert([]byte(word), i)
		tt.Nil(t, err)
	}

	// update the words
	for i, word := range words {
		err := cd.Delete([]byte(word))
		tt.Nil(t, err)

		err = cd.Update([]byte(word), i)
		tt.Nil(t, err)
	}

	// delete not used word
	for i := 10; i < 15; i++ {
		err := cd.Delete([]byte(words[i]))
		tt.Nil(t, err)
	}
}

func TestFind(t *testing.T) {
	cd := New(&Options{
		Reduced: true,
		UseMMap: true,
	})
	defer cd.Close()
	key, err := cd.Find([]byte("a"), 0)
	tt.Nil(t, err)
	tt.Equal(t, 0, key)

	val, err := cd.Get([]byte("ab"))
	tt.Nil(t, err)
	tt.Equal(t, 2, val)

	to, err := cd.Jump([]byte("abc"), 0)
	tt.Nil(t, err)
	tt.Equal(t, 352, to)
	val, err = cd.Value(to)
	tt.Nil(t, err)
	tt.Equal(t, 3, val)
}
