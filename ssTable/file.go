package ssTable

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"sort"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/27 21:38
 * @Func: 与磁盘文件file交互的功能
 **/

//
// writeDataToFile
//  @Description: 创建SSTable用于将SSTable中的数据,索引,元数据等信息落盘
//  @param path
//  @param data
//  @param index
//  @param meta
//
func writeDataToFile(path string, data []byte, index []byte, meta MetaData) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("Fail to create file, ", path, err)
	}
	if _, err = f.Write(data); err != nil {
		log.Fatal("Fail to write data to file", path, err)
	}
	if _, err = f.Write(index); err != nil {
		log.Fatal("Fail to write index to file", path, err)
	}
	// 写入元数据到文件末尾
	// NOTE: 右侧必须能够识别字节长度的类型，不能使用 int 这种类型，只能使用 int32、int64等
	_ = binary.Write(f, binary.LittleEndian, &meta.Version)
	_ = binary.Write(f, binary.LittleEndian, &meta.DataStart)
	_ = binary.Write(f, binary.LittleEndian, &meta.DataLen)
	_ = binary.Write(f, binary.LittleEndian, &meta.IndexStart)
	_ = binary.Write(f, binary.LittleEndian, &meta.IndexLen)
	if err = f.Sync(); err != nil {
		log.Fatal(" Fail to write index to file,", path, err)
	}
	if err = f.Close(); err != nil {
		log.Fatal(" Fail to close file,", path, err)
	}
}

func (s *SSTable) loadFileHandler() {
	if s.F == nil {
		// 如果文件句柄f为空, 则创建一个文件给它
		f, err := os.OpenFile(s.Path, os.O_RDONLY, 0666)
		if err != nil {
			log.Println(" error open file ", s.Path)
			panic(err)
		}
		s.F = f
	}
}

//
// loadIndex
//  @Description: 加载稀疏索引区到内存
//  @receiver s
//
func (s *SSTable) loadIndex() {
	// 根据meta.indexLen读取索引区
	bytes := make([]byte, s.Meta.IndexLen)
	if _, err := s.F.Seek(s.Meta.IndexStart, 0); err != nil {
		log.Println(" error open file ", s.Path)
		panic(err)
	}
	if _, err := s.F.Read(bytes); err != nil {
		log.Println(" error open file ", s.Path)
		panic(err)
	}
	// 反序列化到内存
	s.Index = make(map[string]Position)
	err := json.Unmarshal(bytes, &s.Index)
	if err != nil {
		log.Println(" error open file ", s.Path)
		panic(err)
	}
	_, _ = s.F.Seek(0, 0)

	// 反序列化有序的keys
	keys := make([]string, 0, len(s.Index))
	for k := range s.Index {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	s.Keys = keys
}

//
// loadMetaData
//  @Description: 加载meta, 就是把5个8字节的int64分别加载进来
//  @receiver s
//
func (s *SSTable) loadMetaData() {
	f := s.F
	if _, err := f.Seek(0, 0); err != nil {
		log.Println(" error open file ", s.Path)
		panic(err)
	}

	info, _ := f.Stat() //获取文件大小
	if _, err := f.Seek(info.Size()-8*5, 0); err != nil {
		log.Println("Error reading metadata version", s.Path)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &s.Meta.Version) //先读版本号version

	if _, err := f.Seek(info.Size()-8*4, 0); err != nil {
		log.Println("Error reading metadata dataStart", s.Path)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &s.Meta.DataStart) //再读dataStart

	if _, err := f.Seek(info.Size()-8*3, 0); err != nil {
		log.Println("Error reading metadata dataLen", s.Path)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &s.Meta.DataLen) //再读dataLen

	if _, err := f.Seek(info.Size()-8*2, 0); err != nil {
		log.Println("Error reading metadata indexStart", s.Path)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &s.Meta.IndexStart) //再读indexStart

	if _, err := f.Seek(info.Size()-8*1, 0); err != nil {
		log.Println("Error reading metadata indexLen", s.Path)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &s.Meta.IndexLen) //再读indexLen
}

//
// GetDbSize
//  @Description: 获取SSTable的db文件大小
//  @receiver s
//
func (s *SSTable) GetDbSize() int64 {
	info, err := os.Stat(s.Path)
	if err != nil {
		log.Fatal(err)
	}
	return info.Size()
}
