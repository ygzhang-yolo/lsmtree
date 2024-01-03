package sstTree

import (
	"fmt"
	"github.com/ygzhang-yolo/lsmtree/kv"
	sst "github.com/ygzhang-yolo/lsmtree/ssTable"
	"sync"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/27 19:24
 * @Func: SSTable Tree
 **/

//
//  SSTableTree
//  @Description: SSTable Tree的结构, 包括多个level, 每个level都是一个SSTable的链表
//
type SSTableTree struct {
	levels []*SSTableNode
	mu     *sync.RWMutex
}

//
//  SSTableNode
//  @Description: SSTableNode对应一个SSTable的链表节点
//
type SSTableNode struct {
	index int          //链表的索引
	table *sst.SSTable //链表的值是一个SSTable
	next  *SSTableNode
}

//=============================================核心功能: Get============================================//
//
// Get
//  @Description: Get从所有的SSTable中查找key数据
//  @receiver s
//  @param key
//  @return kv.Value
//  @return kv.SearchResult
//
func (s *SSTableTree) Get(key string) (kv.Value, kv.SearchResult) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 遍历每个level的SSTableNode
	for _, node := range s.levels {
		// 链表转数组, 用tables存这个level里所有的SSTables
		tables := make([]*sst.SSTable, 0)
		for node != nil {
			tables = append(tables, node.table)
			node = node.next
		}
		// 从最新的, 最后一个SSTable开始找
		for i := len(tables) - 1; i >= 0; i-- {
			value, result := tables[i].Get(key)
			if result == kv.None {
				// 如果找不到就找下一个table
				continue
			} else {
				return value, result
			}
		}
	}
	// 全部遍历完都没找到, 返回不存在None
	return kv.Value{}, kv.None
}

//
// CreateTableInLevel
//  @Description: 创建一个新的SSTable, 一般是memTable满了调用, 在level0插入一个新的
//  @receiver s
//  @param values
//
func (s *SSTableTree) CreateTableInLevel(values []kv.Value) {
	s.createTableInLevel(values, 0)
}

//
// createTableInLevel
//  @Description: 创建一个SSTable并插入到level0末尾
//  @receiver s
//  @param values
//  @param level
//  @return *sst.SSTable
//
func (s *SSTableTree) createTableInLevel(values []kv.Value, level int) *sst.SSTable {
	node := s.GetTableNums(level)
	table := sst.NewSSTable(values, level, node) //创建一个SSTable
	s.insert(table, level)                       //根据SSTable创建一个SSTableNode插入到SSTableTree中
	return table
}

//
// insert
//  @Description: 插入一个SSTable到指定层, 并创建对应SSTableNode
//  @receiver s
//  @param table
//  @param level
//
func (s *SSTableTree) insert(table *sst.SSTable, level int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 尾插到链表的最后
	node := s.levels[level]
	sstNode := &SSTableNode{
		index: 0,
		table: table,
		next:  nil,
	}
	if node == nil {
		s.levels[level] = sstNode
	} else {
		//遍历找到链表末尾一个node
		for node.next != nil {
			node = node.next
		}
		sstNode.index = node.index + 1
		node.next = sstNode
	}
}

//=========================================一些辅助函数==========================================//

//
// GetLevelSize
//  @Description: 获取指定层的SSTable大小
//  @receiver s
//  @param level
//  @return int64
//
func (s *SSTableTree) GetLevelSize(level int) int64 {
	var size int64
	cur := s.levels[level]
	for cur != nil {
		size += cur.table.GetDbSize()
		cur = cur.next
	}
	return size
}

//
// GetTableNums
//  @Description: 返回该层有多少个SSTables
//  @receiver s
//  @param level
//  @return int
//
func (s *SSTableTree) GetTableNums(level int) int {
	node := s.levels[level]
	nums := 0
	for node != nil {
		nums++
		node = node.next
	}
	return nums
}

//
// GetLevelFromDB
//  @Description: 获取一个 db 文件所代表的 SSTable 的所在层数和索引
//  @param name
//  @return int
//  @return int
//  @return error
//
func GetLevelFromDB(name string) (int, int, error) {
	var level, index int
	n, err := fmt.Sscanf(name, "%d.%d.db", &level, &index)
	if n != 2 || err != nil {
		return 0, 0, fmt.Errorf("incorrect data file name: %q", name)
	}
	return level, index, nil
}
