package stone

import (
	"errors"
	"hash/crc32"
	"os"
)

const (
	FILE_NAME = "stone.data"
)

type DataFile struct {
	File   *os.File
	Offset int64 // 记录文件每次保存数据后的位置
}

// 创建一个新的数据文件
func NewDataFile(path string) (*DataFile, error) {
	fileName := path + string(os.PathSeparator) + FILE_NAME
	return newInstance(fileName)
}

// 从文件中读取数据
func (df *DataFile) Read(offset int64) (r *Record, err error) {
	buf := make([]byte, recordHeadSize)
	if _, err := df.File.ReadAt(buf, offset); err != nil {
		return nil, err
	}

	r = Decode(buf)

	offset += recordHeadSize
	if r.KeySize > 0 {
		key := make([]byte, r.KeySize)
		if _, err := df.File.ReadAt(key, offset); err != nil {
			return nil, err
		}
		r.Key = key
	}

	offset += int64(r.ValueSize)
	if r.ValueSize > 0 {
		value := make([]byte, r.ValueSize)
		if _, err := df.File.ReadAt(value, offset); err != nil {
			return nil, err
		}
		r.Value = value
	}

	checkCrc := crc32.ChecksumIEEE(r.Value)
	if checkCrc != r.crc {
		return nil, errors.New("invalid crc")
	}
	return r, nil
}

// 将数据写进文件
func (df *DataFile) Write(r *Record) error {
	buf, err := r.Encode()
	if err != nil {
		return err
	}
	if _, err := df.File.WriteAt(buf, df.Offset); err != nil {
		return err
	}
	df.Offset += int64(r.Size())
	return nil
}

func newInstance(fileName string) (*DataFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		File:   file,
		Offset: stat.Size(),
	}, nil
}
