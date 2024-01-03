package kv

import "encoding/json"

/**
 * @Author: ygzhang
 * @Date: 2023/12/26 16:54
 * @Func:
 **/

//=============定义查找结果的结构体SearchResult=============//
type SearchResult int

const (
	None SearchResult = iota
	Deleted
	Success
)

//
//  Value
//  @Description: Value表示一个kv对
//
type Value struct {
	Key     string
	Value   []byte
	Deleted bool
}

//
// Copy
//  @Description: 返回一个Value对应的deep copy
//  @receiver v
//  @return *Value
//
func (v *Value) Copy() *Value {
	return &Value{
		Key:     v.Key,
		Value:   v.Value,
		Deleted: v.Deleted,
	}
}

// NOTE: [T any] 表示这个函数是一个泛型函数，它可以接受任意类型 T 作为参数。
//
// Get[T any]
//  @Description: Get 反序列化元素中的值
//  @param v
//  @return T
//  @return error
//
func Get[T any](v *Value) (T, error) {
	var value T
	err := json.Unmarshal(v.Value, &value)
	return value, err
}

//
// Convert[T any]
//  @Description: Convert 将值序列化为二进制
//  @param value
//  @return []byte
//  @return error
//
func Convert[T any](value T) ([]byte, error) {
	return json.Marshal(value)
}

//
// Decode
//  @Description: Decode 二进制数据反序列化为 Value
//  @param data
//  @return Value
//  @return error
//
func Decode(data []byte) (Value, error) {
	var value Value
	err := json.Unmarshal(data, &value)
	return value, err
}

//
// Encode
//  @Description: Encode 将 Value 序列化为二进制
//  @param value
//  @return []byte
//  @return error
//
func Encode(value Value) ([]byte, error) {
	return json.Marshal(value)
}
