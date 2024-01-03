package sstTree

import (
	"github.com/ygzhang-yolo/lsmtree/ssTable"
	"log"
	"path/filepath"
	"time"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/27 22:00
 * @Func:
 **/

//
// loadFileToMemory
//  @Description: 加载一个db文件到内存中的SSTableTree中
//  @receiver s
//  @param path
//
func (s *SSTableTree) loadFileToMemory(path string) {
	log.Println("Loading the db file into Memory SSTableTree ", path)
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Loading the ", path, ",Consumption of time : ", elapse)
	}()

	// 根据db文件名, 判断level和index
	level, index, err := GetLevelFromDB(filepath.Base(path))
	if err != nil {
		return
	}

	// 创建对应的SSTable对象和SSTableNode
	table := &ssTable.SSTable{}
	table.Init(path)
	node := &SSTableNode{
		index: index,
		table: table,
		next:  nil,
	}

	cur := s.levels[level]
	if cur == nil {
		// 空直接返回
		s.levels[level] = node
		return
	}
	if node.index < cur.index {
		// 如果node比头节点的index还小, 插入到最前面
		// FIXME: 这里应该能用一个dummyHead来统一两种写法
		node.next = cur
		s.levels[level] = node
		return
	}
	// 否则遍历找到index对应的位置插入
	for cur != nil {
		if cur.next == nil || node.index < cur.next.index {
			node.next = cur.next
			cur.next = node
			break
		} else {
			cur = cur.next
		}
	}
}
