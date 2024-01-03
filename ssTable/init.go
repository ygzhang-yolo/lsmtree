package ssTable

import "sync"

/**
 * @Author: ygzhang
 * @Date: 2023/12/29 14:15
 * @Func:
 **/

//
// Init
//  @Description: SSTable的加载函数
//  @receiver s
//  @param path
//
func (s *SSTable) Init(path string) {
	s.Path = path
	s.mu = &sync.Mutex{}
	//从文件中加载SSTable对象

	//从文件中加载SSTable 文件句柄
	s.loadFileHandler()
	// 加载SSTable剩下的两项, 稀疏索引和元数据
	s.loadMetaData()
	s.loadIndex()
}
