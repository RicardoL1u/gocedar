package cedar

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vcaesar/tt"
)

func BenchmarkInsert(t *testing.B) {
	cd := New(&Options{
		Reduced: true,
	})
	fn := func() {
		require.NoError(t, cd.Insert([]byte("a"), 1))
		require.NoError(t, cd.Insert([]byte("b"), 3))
		require.NoError(t, cd.Insert([]byte("d"), 6))
	}

	tt.BM(t, fn)
}

func BenchmarkJump(t *testing.B) {
	fn := func() {
		_, err := cd.Jump([]byte("a"), 1)
		require.NoError(t, err)
	}

	tt.BM(t, fn)
}

func BenchmarkFind(t *testing.B) {
	fn := func() {
		_, err := cd.Find([]byte("a"), 1)
		require.NoError(t, err)
	}

	tt.BM(t, fn)
}

func BenchmarkValue(t *testing.B) {
	fn := func() {
		_, err := cd.Value(1)
		require.NoError(t, err)
	}

	tt.BM(t, fn)
}

func BenchmarkUpdate(t *testing.B) {
	fn := func() {
		require.NoError(t, cd.Update([]byte("a"), 1))
	}

	tt.BM(t, fn)
}

func BenchmarkDelete(t *testing.B) {
	fn := func() {
		require.NoError(t, cd.Delete([]byte("b")))
	}

	tt.BM(t, fn)
}
