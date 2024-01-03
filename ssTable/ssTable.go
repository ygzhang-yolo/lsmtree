package ssTable

import (
	"encoding/json"
	"github.com/ygzhang-yolo/lsmtree/config"
	"github.com/ygzhang-yolo/lsmtree/kv"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/27 19:01
 * @Func:
 **/
//
//  SSTable
//  @Description: SSTable的结构定义, 主要是元数据,
//
type SSTable struct {
	F     *os.File            //文件句柄, 注意os的文件句柄数量有限制
	Path  string              //文件路径
	Meta  MetaData            //元数据
	Index map[string]Position //文件的稀疏索引列表
	Keys  []string            //排序后的key列表
	mu    sync.Locker         //互斥锁
	//keys 是有序的，便于 CPU 缓存等，还可以使用布隆过滤器，有助于快速查找。
	//keys 找到后，使用 index 快速定位
}

/*
SSTable在文件中的存储方式：索引是从数据区开始！
0 ─────────────────────────────────────────────────────────►
◄───────────────────────────
          dataLen          ◄──────────────────
                                indexLen     ◄──────────────┐
┌──────────────────────────┬─────────────────┬──────────────┤
│                          │                 │              │
│          数据区           │   稀疏索引区     │    元数据     │
│                          │                 │              │
└──────────────────────────┴─────────────────┴──────────────┘
*/

//
//  MetaData
//  @Description: MetaData 是 SSTable 的元数据，元数据出现在磁盘文件的末尾
//
type MetaData struct {
	Version    int64 // 版本号
	DataStart  int64 // 数据区起始索引
	DataLen    int64 // 数据区长度
	IndexStart int64 // 稀疏索引区起始索引
	IndexLen   int64 // 稀疏索引区长度
}

//
//  Position
//  @Description: Position元素定位，存储在稀疏索引区中，表示一个元素的起始位置和长度
//
type Position struct {
	Start   int64 // 起始索引
	Len     int64 // 长度
	Deleted bool  // Key 已经被删除
}

//===========================核心功能, Get, 二分法查找key================//
//
// Get
//  @Description: Get查找元素key
//  @receiver s
//  @param key
//  @return kv.Value
//  @return kv.SearchResult
//
func (s *SSTable) Get(key string) (kv.Value, kv.SearchResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos := Position{
		Start: -1,
	}
	// keys是有序的, 可以用二分法查找
	l, r := 0, len(s.Keys)-1
	for l <= r {
		m := l + (r-l)/2
		if s.Keys[m] == key {
			pos = s.Index[key]
			// 判断元素是否已经删除
			if pos.Deleted {
				return kv.Value{}, kv.Deleted
			}
			break
		} else if s.Keys[m] < key {
			l = m + 1
		} else {
			r = m - 1
		}
	}
	// 如果没有查找到, 返回None
	if pos.Start == -1 {
		return kv.Value{}, kv.None
	}

	// 找到了对应的key, 需要从磁盘的数据区拿到数据原始值
	bytes := make([]byte, pos.Len)
	if _, err := s.F.Seek(pos.Start, 0); err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}
	// Read出数据的字节流bytes
	if _, err := s.F.Read(bytes); err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}
	// 反序列化为kv.Value
	value, err := kv.Decode(bytes)
	if err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}
	return value, kv.Success
}

//
// NewSSTable
//  @Description: 根据传入的values, 创建一个对应的SSTable
//  @param values
//
func NewSSTable(values []kv.Value, level int, node int) *SSTable {
	// 生成数据区, 就是把values中所有的value序列化为字节流存起来
	keys := make([]string, 0, len(values)) //记录所有的key
	positions := make(map[string]Position) //记录每个value的起始位置
	data := make([]byte, 0)                //数据区的字节流
	for _, value := range values {
		vdata, _ := kv.Encode(value) //将每个value序列化为二进制
		keys = append(keys, value.Key)
		// 记录字节流的文件偏移, 方便定位
		positions[value.Key] = Position{
			Start:   int64(len(data)),
			Len:     int64(len(vdata)),
			Deleted: value.Deleted,
		}
		data = append(data, vdata...)
	}
	sort.Strings(keys) //对key进行排序, 保证有序的key

	// 生成稀疏索引区
	index, _ := json.Marshal(positions) //序列化为字节流

	// 生成元数据
	var meta = MetaData{
		Version:    0,
		DataStart:  0,
		DataLen:    int64(len(data)),
		IndexStart: int64(len(data)),
		IndexLen:   int64(len(index)),
	}

	// 生成对应的文件句柄
	cfg := config.GetConfig()
	path := cfg.DataDir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(node) + ".db"
	writeDataToFile(path, data, index, meta) //将SSTable数据落盘
	f, _ := os.OpenFile(path, os.O_RDONLY, 0666)

	// 生成SSTable
	table := SSTable{
		F:     f,
		Path:  path,
		Meta:  meta,
		Index: positions,
		Keys:  keys,
		mu:    &sync.RWMutex{},
	}
	return &table
}
