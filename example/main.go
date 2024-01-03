package main

import (
	"bufio"
	"fmt"
	lsm "github.com/ygzhang-yolo/lsmtree"
	"github.com/ygzhang-yolo/lsmtree/config"
	db "github.com/ygzhang-yolo/lsmtree/db"
	"math/rand"
	"os"
	"strconv"
	"time"
)

/**
 * @Author: ygzhang
 * @Date: 2024/1/2 12:25
 * @Func:
 **/

type TestValue struct {
	A int64
	B int64
	C int64
	D string
}

func main() {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println(r)
			inputReader := bufio.NewReader(os.Stdin)
			_, _ = inputReader.ReadString('\n')
		}
	}()
	lsm.Start(config.Config{
		DataDir:       `D:\study\杂项\lsmData`,
		Level0Size:    100,
		PartSize:      4,
		Threshold:     10000,
		CheckInterval: 3,
	})

	fmt.Println(db.DB)

	//---------------some tests---------------------//
	//basicGetAndSet()
	//crashWithWal()
	//writeSSTable()
	sstCompaction()

	//query("a" + strconv.Itoa(89))

	// main函数不终止保证后天监视协程能得到定期执行
	for {
		time.Sleep(time.Second)
	}
}

func basicGetAndSet() {
	fmt.Println("-------------------Test BasicGetAndSet()--------------------")
	// 先query aaa
	start := time.Now()
	v, _ := lsm.Get[TestValue]("aaa")
	elapse := time.Since(start)
	fmt.Println("查找 aaaaaa 完成，消耗时间：", elapse)
	fmt.Println(v)

	// 再set aaa
	lsm.Set[TestValue]("aaa", TestValue{
		A: 1,
		B: 1,
		C: 1,
		D: "aaaaaa",
	})

	// 再query
	start = time.Now()
	v, _ = lsm.Get[TestValue]("aaa")
	elapse = time.Since(start)
	fmt.Println("查找 aaaaaa 完成，消耗时间：", elapse)
	fmt.Println(v)
}

func crashWithWal() {
	fmt.Println("-------------------Test CrashWithWal()--------------------")
	// 直接query aaa, 内存表会根据wal.log重建
	start := time.Now()
	v, _ := lsm.Get[TestValue]("aaa")
	elapse := time.Since(start)
	fmt.Println("查找 aaaaaa 完成，消耗时间：", elapse)
	fmt.Println(v)
}

func writeSSTable() {
	fmt.Println("-------------------Test WriteSSTable()--------------------")
	// 插入100条数据
	entryNums := 200
	for i := 0; i < entryNums; i++ {
		key := "a" + strconv.Itoa(i)
		value := TestValue{
			A: int64(i),
			B: int64(i),
			C: int64(i),
			D: "a" + strconv.Itoa(i),
		}
		lsm.Set[TestValue](key, value)
	}

	// 休眠 2 * CheckInterval 保证SSTable压缩一定会执行
	cfg := config.GetConfig()
	time.Sleep(2 * time.Duration(cfg.CheckInterval) * time.Second)

	// 随机选择一个Get
	// 设置种子，以确保每次运行程序时都能生成不同的随机数
	rand.Seed(time.Now().UnixNano())
	// 生成0到99之间的随机数
	randomNumber := rand.Intn(100)
	query("a" + strconv.Itoa(randomNumber))
}

func sstCompaction() {
	fmt.Println("-------------------Test SSTableCompaction()--------------------")
	// 插入100条数据, 共
	entryNums := 100
	for i := 0; i < entryNums; i++ {
		key := "a" + strconv.Itoa(i)
		value := TestValue{
			A: int64(i),
			B: int64(i),
			C: int64(i),
			D: "a" + strconv.Itoa(i),
		}
		lsm.Set[TestValue](key, value)
		time.Sleep(time.Second)
	}

	// 休眠 2 * CheckInterval 保证SSTable压缩一定会执行
	cfg := config.GetConfig()
	time.Sleep(2 * time.Duration(cfg.CheckInterval) * time.Second)

	// 随机选择一个Get
	// 设置种子，以确保每次运行程序时都能生成不同的随机数
	rand.Seed(time.Now().UnixNano())
	// 生成0到99之间的随机数
	randomNumber := rand.Intn(100)
	query("a" + strconv.Itoa(randomNumber))
}

func query(key string) {
	start := time.Now()
	v, _ := lsm.Get[TestValue](key)
	elapse := time.Since(start)
	fmt.Println("查找 ", key, "完成，消耗时间：", elapse)
	fmt.Println(v)
}
