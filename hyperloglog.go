package goRedis

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

const seed = 0xadc83b19

const hll_dense = 1
const hll_sparse = 2

const hll_p = 14
const hll_q = 64 - hll_p
const hll_registers = 1 << hll_p
const hll_p_mask = hll_registers - 1
const hll_bits = 6
const hll_sparse_val_max_value = 32
const hll_alpha_inf = 0.721347520444481703680

type hllhdr struct {
	magic      string
	encoding   uint8
	notused    [3]uint8
	card       [8]uint64
	registers  []byte //实际存储的，因为后面如果encoding方式采用sparse的话，长度会变化，所以使用slice比较好
	vaildCache bool
}

func initHLL(encoding uint8) *hllhdr {
	hdr := new(hllhdr)
	hdr.magic = "HYLL"
	hdr.encoding = encoding

	if encoding == hll_dense {
		hdr.registers = make([]byte, hll_registers*1) // 先简单实现下 用一个字节存6个bit
	} else {
		panic("HLL SPARSE encoding format doesn't support.")
	}
	return hdr
}

func hllDenseSet(hllObj *hllhdr, index uint64, count int) bool {
	if count > int(hllObj.registers[index]) {
		hllObj.registers[index] = byte(count)
		return true
	}
	return false
}

func PfAddCommand(hllObj *hllhdr, val []byte) {
	index, count := hllPartLen(val)
	if hllObj.encoding == hll_dense {
		hllDenseSet(hllObj, index, count)
		hllObj.vaildCache = false
	} else {
		panic("HLL SPARSE encoding format doesn't support.")
	}
}

func hllTau(x float64) float64 {
	if x == 0. || x == 1. {
		return 0.
	}
	var zPrime float64
	y := 1.0
	z := 1 - x
	for {
		x = math.Sqrt(x)
		zPrime = z
		y *= 0.5
		z -= math.Pow(1-x, 2) * y
		if zPrime == z {
			break
		}
	}
	return z / 3

}

func hllDenseRegHisto(hllObj *hllhdr, reghisto *[hll_q + 2]int) {
	for i := 0; i < hll_registers; i++ {
		reg := hllObj.registers[i]
		reghisto[reg]++
	}
}

func hllSigma(x float64) float64 {
	if x == 1. {
		return math.MaxInt64
	}
	var zPrime float64
	y := float64(1)
	z := x
	for {
		x *= x
		zPrime = z
		z += x * y
		y += y
		if zPrime == z {
			break
		}
	}
	return z
}

func hllCount(hllObj *hllhdr) int {
	m := float64(hll_registers)
	var reghisto [hll_q + 2]int
	if hllObj.encoding == hll_dense {
		hllDenseRegHisto(hllObj, &reghisto)
	} else {
		panic("impliment me..")
	}

	z := m * hllTau((m - (float64(reghisto[hll_q+1]))/m))
	for j := hll_q; j >= 1; j-- {
		z += float64(reghisto[j])
		z *= 0.5
	}
	z += m * hllSigma(float64(reghisto[0])/m)
	E := math.Round(hll_alpha_inf * m * m / z)

	return int(E)

}

func PfCountCommand(hllObj *hllhdr) int {
	var ret int
	if hllObj.vaildCache {
		return 0
	} else {
		ret = hllCount(hllObj)
	}

	return ret
}

func CreateHLLObject() *hllhdr {
	hdr := initHLL(hll_dense)
	return hdr
}

func Murmurhash(buff []byte, seed uint32) uint64 {
	buffLen := uint64(len(buff))
	m := uint64(0xc6a4a7935bd1e995)
	r := uint32(47)
	h := uint64(seed) ^ (buffLen * m)

	for i := uint64(0); i < buffLen-(buffLen&7); {
		var k uint64
		bBuffer := bytes.NewBuffer(buff[i : i+8])
		binary.Read(bBuffer, binary.LittleEndian, &k)

		k *= m
		k ^= k >> r
		k *= m
		h ^= k
		h *= m

		binary.Write(bBuffer, binary.LittleEndian, &k)
		i += 8
	}
	switch buffLen & 7 {
	case 7:
		h ^= uint64(buff[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(buff[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(buff[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(buff[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(buff[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(buff[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(buff[0])
		fallthrough
	default:
		h *= m
	}

	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}

func hllPartLen(buff []byte) (index uint64, count int) {
	hash := Murmurhash(buff, seed)
	index = hash & uint64(hll_p_mask) //这里就是取出后14个bit，作为index
	hash >>= hll_p                    //右移把后面14个bit清理掉,注意这里的bit流其实是倒序的
	hash |= uint64(1) << hll_q        //当前的最高位设置1，其实是一个哨兵，避免count为0
	bit := uint64(1)
	count = 1
	for (hash & bit) == 0 {
		count++
		bit <<= 1
	}
	fmt.Printf("pf hash idx=%d, count=%d\n", index, count)

	return index, count
}

//func hllSparseSet(o, index int64, count int64) {
//	if count > hll_sparse_val_max_value {
//		goto promote
//	}
//
//promote:
//}
