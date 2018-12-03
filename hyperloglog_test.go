package goRedis

import "testing"

func TestMurmurhash(t *testing.T) {
	str := "apple"
	appleExpert := uint64(10303303131529439706)

	hashVal := Murmurhash([]byte(str), 0xadc83b19)
	if hashVal != appleExpert {
		t.Errorf("Murmurhash error.")
	}
}

func TesthllPartLen(t *testing.T) {
	str := "apple"
	idx, count := hllPartLen([]byte(str))
	print(idx, count)
}

func TestAll(t *testing.T) {
	hllObj := CreateHLLObject()
	test1 := []string{"apple", "apple", "orange", "ttt", "aaa"}

	for _, str := range test1 {
		PfAddCommand(hllObj, []byte(str))
	}

	println(PfCountCommand(hllObj))
}
