package config

import "sync"

/**
 * @Author: ygzhang
 * @Date: 2023/12/27 21:26
 * @Func:
 **/

// Config 数据库启动配置
type Config struct {
	DataDir       string // 数据目录
	Level0Size    int    // 0 层的 所有 SsTable 文件大小总和的最大值，单位 MB，超过此值，该层 SsTable 将会被压缩到下一层
	PartSize      int    // 每层中 SsTable 表数量的阈值，该层 SsTable 将会被压缩到下一层
	Threshold     int    // 内存表的 kv 最大数量，超出这个阈值，内存表将会被保存到 SsTable 中
	CheckInterval int    // 压缩内存、文件的时间间隔，多久进行一次检查工作
}

// 单例模式
var once *sync.Once = &sync.Once{}

// 常驻内存
var config Config

// Init 初始化数据库配置
func Init(con Config) {
	once.Do(func() {
		config = con
	})
}

// GetConfig 获取数据库配置
func GetConfig() Config {
	return config
}
