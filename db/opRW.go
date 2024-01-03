package db

import (
	"encoding/json"
	"github.com/ygzhang-yolo/lsmtree/kv"
	"log"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/29 18:13
 * @Func:
 **/

//
// Get[T any]
//  @Description: 数据查询过程Get
//  @param key
//  @return T
//  @return bool
//
func Get[T any](key string) (T, bool) {
	log.Print("Get ", key)
	// 1. 先查内存表, 查询成功直接返回
	value, result := DB.MemoryTree.Get(key)
	if result == kv.Success {
		return getInstanceFromBytes[T](value.Value)
	}

	// 2. 查SSTable文件
	if DB.SSTableTree != nil {
		value, result = DB.SSTableTree.Get(key)
		if result == kv.Success {
			return getInstanceFromBytes[T](value.Value)
		}
	}
	// 否则只能返回空
	var nilValue T
	return nilValue, false
}

//
// Set[T any]
//  @Description: Set 插入元素
//  @param key
//  @param value
//  @return bool
//
func Set[T any](key string, value T) bool {
	log.Print("Insert ", key, ",")
	data, err := kv.Convert(value) //将value序列化为二进制
	if err != nil {
		log.Println(err)
		return false
	}

	// 1.先写入database
	_, _ = DB.MemoryTree.Set(key, data)

	// 2.再写入 wal.log
	DB.Wal.Write(kv.Value{
		Key:     key,
		Value:   data,
		Deleted: false,
	})
	return true
}

//
// DeleteAndGet[T any]
//  @Description: // DeleteAndGet 删除元素并尝试获取旧的值， 返回的 bool 表示是否有旧值，不表示是否删除成功
//  @param key
//  @return T
//  @return bool
//
func DeleteAndGet[T any](key string) (T, bool) {
	log.Print("Delete ", key)
	value, success := DB.MemoryTree.Delete(key)

	if success {
		// 写入 wal.log
		DB.Wal.Write(kv.Value{
			Key:     key,
			Value:   nil,
			Deleted: true,
		})
		return getInstanceFromBytes[T](value.Value)
	}
	var nilV T
	return nilV, false
}

//
// Delete[T any]
//  @Description: 单纯的Delete删除元素
//  @param key
//
func Delete[T any](key string) {
	log.Print("Delete ", key)
	DB.MemoryTree.Delete(key)
	DB.Wal.Write(kv.Value{
		Key:     key,
		Value:   nil,
		Deleted: true,
	})
}

//
// getInstanceFromBytes[T any]
//  @Description: 将字节数组转为类型对象
//  @param data
//  @return T
//  @return bool
//
func getInstanceFromBytes[T any](data []byte) (T, bool) {
	var value T
	err := json.Unmarshal(data, &value)
	if err != nil {
		log.Println(err)
	}
	return value, true
}
