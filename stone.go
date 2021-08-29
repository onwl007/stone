package stone

import (
	"errors"
	"io"
	"os"
)

type DB struct {
	df  *DataFile        // 活跃数据文件
	idx map[string]int64 // 内存中的索引信息
}

// 1. 打开数据文件
// 2. 如果不存在则创建
// 3. 读取数据文件中的数据
// 4. 建立索引
// 5. 返回 db 对象
func Open(dirPath string) (*DB, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 加载数据文件
	df, err := NewDataFile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &DB{
		df:  df,
		idx: make(map[string]int64),
	}

	db.loadIndex(df)
	return db, nil
}

func (d *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("key not empty")
	}

	offset := d.df.Offset
	r := NewRecord(key, value)
	err := d.df.Write(r)
	if err != nil {
		return err
	}

	d.idx[string(key)] = offset
	return nil
}

func (d *DB) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}

	offset, ok := d.idx[string(key)]
	if !ok {
		return
	}

	var r *Record
	r, err = d.df.Read(offset)
	if err != nil && err != io.EOF {
		return
	}

	if r != nil {
		val = r.Value
	}
	return
}

func (d *DB) loadIndex(df *DataFile) {
	if df == nil {
		return
	}

	var offset int64
	for {
		r, err := d.df.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}

		d.idx[string(r.Key)] = offset
		offset += int64(r.Size())
	}
	return
}
