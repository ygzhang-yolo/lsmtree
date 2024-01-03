package db

import (
	"github.com/ygzhang-yolo/lsmtree/bst"
	"github.com/ygzhang-yolo/lsmtree/sstTree"
	"github.com/ygzhang-yolo/lsmtree/wal"
	"log"
	"os"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/29 15:39
 * @Func: Database, 对外提供的kv db
 **/

type Database struct {
	// 内存表
	MemoryTree *bst.BSTree
	// SSTable 列表
	SSTableTree *sstTree.SSTableTree
	// WalF 文件句柄
	Wal *wal.Wal
}

// 单例模式, 数据库，全局唯一实例
var DB *Database

//
// InitDatabase
//  @Description: 初始化Database, 从磁盘中还原SSTableTree, WAL, MemoryTable
//  @param dir
//
func InitDatabase(dir string) {
	DB = &Database{
		MemoryTree:  &bst.BSTree{},
		SSTableTree: &sstTree.SSTableTree{},
		Wal:         &wal.Wal{},
	}

	// 从磁盘中恢复数据, 如果目录为空, 说明是空数据库, 要新建
	if _, err := os.Stat(dir); err != nil {
		log.Printf("The %s directory does not exist. The directory is being created\r\n", dir)
		err = os.Mkdir(dir, 0666)
		if err != nil {
			log.Println("Failed to create the database directory")
			panic(err)
		}
	}

	//非空数据库, 加载WAL和database文件
	// memTable要通过WAL来创建, 因为可能需要根据WAL中记录的数据恢复memTable
	memTree := DB.Wal.Init(dir)
	DB.MemoryTree = memTree
	log.Println("Loading database...")
	DB.SSTableTree.Init(dir)
}
