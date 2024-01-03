package sstTree

import (
	"github.com/ygzhang-yolo/lsmtree/bst"
	"github.com/ygzhang-yolo/lsmtree/config"
	"github.com/ygzhang-yolo/lsmtree/kv"
	"log"
	"os"
	"time"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/29 16:30
 * @Func: SSTable Tree的压缩
 **/

//
// Check
//  @Description: 检查是否需要压缩数据库文件
//  @receiver s
//
func (s *SSTableTree) Check() {
	s.majorCompact()
}

func (s *SSTableTree) majorCompact() {
	cfg := config.GetConfig()
	for levelIndex, _ := range s.levels {
		tableSize := int(s.GetLevelSize(levelIndex) / 1000 / 1000) //转为MB单位
		// 检查当前level的SSTable总大小和总个数是否超出阈值
		if s.GetTableNums(levelIndex) > cfg.PartSize || tableSize > levelMaxSize[levelIndex] {
			// 需要对SSTable进行压缩compact
			s.majorCompactLevel(levelIndex)
		}
	}
}

//
// majorCompactLevel
//  @Description: 对level层进行Compact, 压缩当前层的文件到下一层
//  @receiver s
//  @param level
//
func (s *SSTableTree) majorCompactLevel(level int) {
	log.Println("Compressing layer ", level, " files")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Completed compression,consumption of time : ", elapse)
	}()
	//-------------compact start--------------------//
	log.Printf("Compressing layer %d.db files\r\n", level)
	tableMem := make([]byte, levelMaxSize[level]) //将所有SSTable都加载到内存
	cur := s.levels[level]
	// 将level层的所有SSTable合并到一个BST中
	memTree := bst.NewBSTree()
	s.mu.Lock()
	for cur != nil {
		table := cur.table
		// 数据区加载到内存中
		// 注意如果数据区长度dataLen更大, 要对tableMem进行扩容
		if int64(len(tableMem)) < table.Meta.DataLen {
			tableMem = make([]byte, table.Meta.DataLen)
		}
		newSlice := tableMem[0:table.Meta.DataLen]
		// 读取数据区
		if _, err := table.F.Seek(0, 0); err != nil {
			log.Println(" error open file ", table.Path)
			panic(err)
		}
		if _, err := table.F.Read(newSlice); err != nil {
			log.Println(" error read file ", table.Path)
			panic(err)
		}
		// 从稀疏索引表中记录的每一个Value, 设置对应的memTree
		for k, pos := range table.Index {
			// 根据是否删除, 调用Delete和Set方法();
			if pos.Deleted {
				memTree.Delete(k)
			} else {
				value, err := kv.Decode(newSlice[pos.Start:(pos.Start + pos.Len)]) //还原每一个Value
				if err != nil {
					log.Fatal(err)
				}
				memTree.Set(k, value.Value)
			}
		}
		cur = cur.next
	}
	s.mu.Unlock()

	//
	values := memTree.GetKV()
	nextLevel := level + 1
	// 不能超出level Max Num限制
	if nextLevel >= levelMaxNum {
		nextLevel = levelMaxNum
	}
	// 在新层创建新的SSTable
	s.createTableInLevel(values, nextLevel)
	// 旧层删掉现有的, 最底层不能删
	oldNodeData := s.levels[level]
	if level < levelMaxNum {
		s.levels[level] = nil
		s.freeLevelData(oldNodeData)
	}
}

//
// freeLevelData
//  @Description: 释放清理掉level层的数据
//  @receiver s
//  @param node
//
func (s *SSTableTree) freeLevelData(node *SSTableNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 遍历链表, 释放每个链表的内存
	for node != nil {
		// 关闭文件
		if err := node.table.F.Close(); err != nil {
			log.Println(" error close file,", node.table.Path)
			panic(err)
		}
		//删除文件
		if err := os.Remove(node.table.Path); err != nil {
			log.Println(" error delete file,", node.table.Path)
			panic(err)
		}
		//置空指针
		node.table.F = nil
		node.table = nil
		node = node.next
	}
}
