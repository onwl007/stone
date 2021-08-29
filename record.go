package stone

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
)

const (
	recordHeadSize = 20
)

type Record struct {
	crc       uint32 // 校验码
	Tstamp    uint64 // 时间戳
	KeySize   uint32 // key 大小
	ValueSize uint32 // value 大小
	Key       []byte
	Value     []byte
}

func NewRecord(key, value []byte) *Record {
	return &Record{
		Tstamp:    uint64(time.Now().UnixNano()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Key:       key,
		Value:     value,
	}
}

func (r *Record) Size() uint32 {
	return recordHeadSize + r.KeySize + r.ValueSize
}

// 序列化成二进制
func (r *Record) Encode() ([]byte, error) {
	if r == nil {
		return nil, errors.New("record is nil")
	}

	buf := make([]byte, r.Size())
	binary.BigEndian.PutUint64(buf[4:12], r.Tstamp)
	binary.BigEndian.PutUint32(buf[12:16], r.KeySize)
	binary.BigEndian.PutUint32(buf[16:20], r.ValueSize)
	copy(buf[recordHeadSize:recordHeadSize+r.KeySize], r.Key)
	copy(buf[recordHeadSize+r.KeySize:recordHeadSize+r.KeySize+r.ValueSize], r.Value)

	crc := crc32.ChecksumIEEE(r.Value)
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

// 反序列化，返回结构体
func Decode(buf []byte) *Record {
	crc := binary.BigEndian.Uint32(buf[0:4])
	tstamp := binary.BigEndian.Uint64(buf[4:12])
	ks := binary.BigEndian.Uint32(buf[12:16])
	vs := binary.BigEndian.Uint32(buf[16:20])

	return &Record{
		crc:       crc,
		Tstamp:    tstamp,
		KeySize:   ks,
		ValueSize: vs,
	}
}
