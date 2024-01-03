package sstTree

import (
	"github.com/ygzhang-yolo/lsmtree/config"
	"io/ioutil"
	"log"
	"path"
	"sync"
	"time"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/29 14:09
 * @Func:
 **/

var levelMaxSize []int

const levelMaxNum = 10 //最大层数为10

//
// Init
//  @Description: 初始化SSTableTree
//  @receiver s
//  @param dir
//
func (s *SSTableTree) Init(dir string) {
	log.Println("The SSTable list are being loaded")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("The SSTable list are being loaded,consumption of time : ", elapse)
	}()

	// 初始化每一层 SSTable 的文件总最大值, 每个是上一层的10倍大小
	con := config.GetConfig()
	levelMaxSize = make([]int, levelMaxNum)
	levelMaxSize[0] = con.Level0Size
	levelMaxSize[1] = levelMaxSize[0] * 10
	levelMaxSize[2] = levelMaxSize[1] * 10
	levelMaxSize[3] = levelMaxSize[2] * 10
	levelMaxSize[4] = levelMaxSize[3] * 10
	levelMaxSize[5] = levelMaxSize[4] * 10
	levelMaxSize[6] = levelMaxSize[5] * 10
	levelMaxSize[7] = levelMaxSize[6] * 10
	levelMaxSize[8] = levelMaxSize[7] * 10
	levelMaxSize[9] = levelMaxSize[8] * 10

	// 初始化SSTable Tree的成员
	s.levels = make([]*SSTableNode, 10)
	s.mu = &sync.RWMutex{}

	// 检查路径下的db文件, 如果有要加载到内存中
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Println("Failed to read the database file")
		panic(err)
	}
	for _, info := range infos {
		// 如果是SSTable的db文件, 将其加载到内存中的SSTable中
		if path.Ext(info.Name()) == ".db" {
			s.loadFileToMemory(path.Join(dir, info.Name()))
		}
	}
}
