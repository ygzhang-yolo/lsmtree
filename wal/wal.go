package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/ygzhang-yolo/lsmtree/bst"
	"github.com/ygzhang-yolo/lsmtree/kv"
	"log"
	"os"
	"path"
	"sync"
	"time"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/27 15:46
 * @Func: 实现LSM中的WAL, Write-Ahead-Lock
 **/

//
//  Wal
//  @Description: WAL的对象
//
type Wal struct {
	f    *os.File    //保存的文件句柄
	path string      //保存的文件路径
	mu   sync.Locker //保证文件资源互斥访问的锁
}

const walName = "wal.log" //定义wal log文件的默认日志名为wal.log

//
// Init
//  @Description: WAL对应的初始化操作
//  @receiver w
//  @param dir
//  @return *bst.BSTree
//
func (w *Wal) Init(dir string) *bst.BSTree {
	log.Printf("Loading Wal log from file %v", walName)
	// 统计启动的时间
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Loaded Wal log finished, total time: ", elapse)
	}()
	// 创建对应的wal.log文件
	walPath := path.Join(dir, walName)
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Printf("The %v file created failed", walName)
		panic(err)
	}
	w.f = f
	w.path = walPath
	w.mu = &sync.Mutex{}
	// 将wal.log文件加载到内存
	return w.loadMemory()
}

//
// loadMemory
//  @Description: 解析将wal.log文件的日志加载到内存, 建立MemTable
//  @receiver w
//  @return *bst.BSTree
//
func (w *Wal) loadMemory() *bst.BSTree {
	w.mu.Lock()
	defer w.mu.Unlock()

	memTable := bst.NewBSTree()
	info, _ := os.Stat(w.path)
	size := info.Size() //文件大小

	// 如果log文件为空, 返回空memTable
	if size == 0 {
		return &memTable
	}
	// NOTE: 从头读取日志文件, 必须要重置文件指针; Seek(0,0)设置文件读写指针移动到开始位置
	_, err := w.f.Seek(0, 0)
	if err != nil {
		log.Println("Failed to open the wal.log")
		panic(err)
	}
	// 同理, 只要文件打开成功, 读取结束, 要将文件指针移到最后, 方便下次追加
	defer func(f *os.File, offset int64, whence int) {
		_, err := f.Seek(offset, whence)
		if err != nil {
			log.Println("Failed to open the wal.log")
			panic(err)
		}
	}(w.f, size-1, 0)
	// 将log文件中的数据全部读到内存
	data := make([]byte, size)
	_, err = w.f.Read(data)
	if err != nil {
		log.Println("Failed to read the wal.log")
		panic(err)
	}

	bodyLen := int64(0) //log每一项entry的长度
	index := int64(0)   //遍历data的索引
	for index < size {
		// 前8字节header代表每一项Value的大小, 先提取出每一项entry的长度
		headerData := data[index:(index + 8)]
		buf := bytes.NewBuffer(headerData)                    //创建字节缓冲区
		err = binary.Read(buf, binary.LittleEndian, &bodyLen) //将headerData中的内容读到entryLen中
		if err != nil {
			log.Println("Fail to read header entryLen from buffer")
			panic(err)
		}
		// 根据entryLen, 提取出entry的字节并还原为Value
		index += 8
		bodyData := data[index:(index + bodyLen)]
		var value kv.Value
		err = json.Unmarshal(bodyData, &value)
		if err != nil {
			log.Println("Fail to umarshal entry data from buffer")
			panic(err)
		}
		// 根据Value的类型, 插入到MemTable中完成还原
		if value.Deleted == true {
			memTable.Delete(value.Key)
		} else {
			memTable.Set(value.Key, value.Value)
		}
		// 遍历下一个entry
		index = index + bodyLen
	}
	return &memTable
}

//
// Write
//  @Description: 执行写入操作时需要同步执行的Write写日志
//  @receiver w
//  @param value
//
func (w *Wal) Write(value kv.Value) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if value.Deleted {
		log.Println("wal.log:	delete ", value.Key)
	} else {
		log.Println("wal.log:	set ", value.Key)
	}
	// 将value序列化为二进制数据
	body, _ := json.Marshal(value)
	header := len(body)
	// 先写入value的长度作为header, 再写入数据作为body
	err := binary.Write(w.f, binary.LittleEndian, int64(header))
	err = binary.Write(w.f, binary.LittleEndian, body)
	if err != nil {
		log.Printf("Fail to Write value=%v to log", value)
		panic(err)
	}
}

//
// Reset
//  @Description: 重置日志文件, 用来在memTable满了要落盘的时候, 重置wal
//  @receiver w
//
func (w *Wal) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	log.Println("Resetting the wal.log file")

	_ = w.f.Close() // 关闭文件句柄

	w.f = nil
	_ = os.Remove(w.path) //删除文件

	// 创建一个空的新文件
	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	w.f = f
}
