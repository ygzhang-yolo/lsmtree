package lsmtree

import (
	"github.com/ygzhang-yolo/lsmtree/config"
	"github.com/ygzhang-yolo/lsmtree/db"
	"github.com/ygzhang-yolo/lsmtree/monitor"
	"log"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/29 15:33
 * @Func:
 **/

func Start(cfg config.Config) {
	// 保证database只能启动一次
	if db.DB != nil {
		return
	}
	// 初始化配置, 将配置保存到内存
	log.Println("Loading a Configuration File")
	config.Init(cfg)

	// 初始化kv数据库databse
	db.InitDatabase(cfg.DataDir)

	// 检查内存和数据库文件
	monitor.CheckMemory()
	db.DB.SSTableTree.Check()

	// 开启后台监视线程, 周期性检查memTable和SSTable大小
	go monitor.Monitor()
}

// 对外提供的方法的封装...
func Get[T any](key string) (T, bool) {
	return db.Get[T](key)
}

func Set[T any](key string, value T) bool {
	return db.Set[T](key, value)
}

func DeleteAndGet[T any](key string) (T, bool) {
	return db.DeleteAndGet[T](key)
}

func Delete[T any](key string) {
	db.Delete[T](key)
}
