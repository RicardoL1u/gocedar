package cedar

import (
	"bufio"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/go-ego/cedar"
	"github.com/stretchr/testify/require"
)

const (
	sourceFile = "/mnt/cbs/data/song_name_500w" // a file contains one key per line
	dumpPath   = "./dump_path"                  // a path to the dumped file
)

func TestMDumpAndLoad(t *testing.T) {
	if _, err := os.Stat(dumpPath); err != nil {
		require.NoError(t, os.MkdirAll(dumpPath, 0644))
	}

	sf, err := os.Open(sourceFile)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, sf.Close())
	}()

	reader := bufio.NewReader(sf)

	cd := cedar.New()
	t1 := time.Now()
	var index int
	lines := make([][]byte, 0)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			log.Printf("ReadBytes fail, err %v\n", err)
			break
		}
		err = cd.Insert(line, index)
		lines = append(lines, line)
		if err != nil {
			log.Printf("Insert fail, line %s, err %v\n", line, err)
			break
		}
		index++
	}
	log.Printf("go-gse/cedar insert %d keys cost %v", index, time.Since(t1))

	t1 = time.Now()
	for _, w := range lines {
		_, err = cd.Get(w)
		require.NotEqual(t, cedar.ErrNoPath, err)
	}
	log.Printf("go-gse/cedar Search cost %v", time.Since(t1))

	t1 = time.Now()
	err = cd.SaveToFile(path.Join(dumpPath, "trie_gob"), "gob")
	require.NoError(t, err)
	log.Printf("go-gse/cedar SaveToFile with gob cost %v", time.Since(t1))

	gocedar := New(&Options{
		UseMMap:  true,
		MMapPath: dumpPath,
	})
	t1 = time.Now()
	index = 0
	for i, line := range lines {
		index++
		err = gocedar.Insert(line, i)
		require.NoError(t, err)
	}

	log.Printf("gocedar insert %d keys cost %v", index, time.Since(t1))

	t1 = time.Now()
	gocedar.Close()
	log.Printf("gocedar close cost %v", time.Since(t1))

}
func TestMLoadFromGob(t *testing.T) {
	_, err := os.Stat(dumpPath)
	require.NoError(t, err)

	t1 := time.Now()
	jcd := cedar.New()
	err = jcd.LoadFromFile(path.Join(dumpPath, "trie_gob"), "gob")
	require.NoError(t, err)
	log.Printf("go-gse/cedar LoadFromFile with gob cost %v", time.Since(t1))

	sf, err := os.Open(sourceFile)
	require.NoError(t, err)
	defer sf.Close() // nolint
	reader := bufio.NewReader(sf)

	t1 = time.Now()
	var index int
	var maxCost time.Duration
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			log.Printf("ReadBytes fail, err %v\n", err)
			break
		}
		t2 := time.Now()
		v, err := jcd.Get(line)
		require.NoError(t, err)
		require.Equal(t, index, v)
		index++
		if time.Since(t2) > maxCost {
			maxCost = time.Since(t2)
		}
	}
	log.Printf("gob cedar Search cost %v, maxCost %v", time.Since(t1), maxCost)
}

func TestMLoadFromMMap(t *testing.T) {
	sf, err := os.Open(sourceFile)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, sf.Close())
	}()
	reader := bufio.NewReader(sf)

	t1 := time.Now()
	ngocedar := New(&Options{
		UseMMap:  true,
		MMapPath: dumpPath,
	})
	log.Printf("gocedar load from mmap cost %v", time.Since(t1))

	t1 = time.Now()
	var index int
	var maxCost time.Duration
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			log.Printf("ReadBytes fail, err %v\n", err)
			break
		}
		t2 := time.Now()
		v, err := ngocedar.Get(line)
		require.NoError(t, err)
		require.Equal(t, index, v)
		index++
		if maxCost < time.Since(t2) {
			maxCost = time.Since(t2)
		}
	}
	log.Printf("gocedar Search cost %v, maxSingleCost %v", time.Since(t1), maxCost)
}
