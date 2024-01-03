package monitor

import (
	"github.com/ygzhang-yolo/lsmtree/config"
	"github.com/ygzhang-yolo/lsmtree/db"
	"log"
	"time"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/29 15:55
 * @Func:
 **/

func Monitor() {
	cfg := config.GetConfig()
	ticker := time.Tick(time.Duration(cfg.CheckInterval) * time.Second)
	for _ = range ticker {
		// 检查内存表是否超出大小限制, 需要落盘生成SSTable
		CheckMemory()
		// 检查数据文件是否过大, 需要压缩compaction
		db.DB.SSTableTree.Check()
	}
}

func CheckMemory() {
	cfg := config.GetConfig()
	// 检查内存表大小是否超过限制
	count := db.DB.MemoryTree.GetCount()
	if count < cfg.Threshold {
		return
	}
	// 内存表过大, 需要转为SSTable存储
	log.Println("Compressing memory")
	tmpTree := db.DB.MemoryTree.Swap()

	// 将内存表存储到 SsTable 中
	db.DB.SSTableTree.CreateTableInLevel(tmpTree.GetKV())
	db.DB.Wal.Reset()
}
