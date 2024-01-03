package kv

import (
	"reflect"
	"testing"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/26 17:12
 * @Func:
 **/

type vTest struct {
	A int
	B int
}

var testData []byte = []byte{123, 34, 65, 34, 58, 49, 50, 51, 44, 34, 66, 34, 58, 49, 50, 51, 125}

func TestConvert(t *testing.T) {
	type args[T any] struct {
		value T
	}
	type testCase[T any] struct {
		name    string
		args    args[T]
		want    []byte
		wantErr bool
	}
	tests := []testCase[vTest]{
		{
			name: "testConvert",
			args: args[vTest]{
				value: vTest{
					A: 123,
					B: 123,
				},
			},
			want:    testData,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Convert(tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Convert() got = %v, want %v", got, tt.want)
			}
		})
	}
}

//func TestDecode(t *testing.T) {
//	type args struct {
//		data []byte
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    Value
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := Decode(tt.args.data)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("Decode() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("Decode() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestEncode(t *testing.T) {
//	type args struct {
//		value Value
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    []byte
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := Encode(tt.args.value)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("Encode() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("Encode() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestGet(t *testing.T) {
//	type args struct {
//		v *Value
//	}
//	type testCase[T any] struct {
//		name    string
//		args    args
//		want    T
//		wantErr bool
//	}
//	tests := []testCase[ /* TODO: Insert concrete types here */ ]{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := Get(tt.args.v)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("Get() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestValue_copy(t *testing.T) {
//	type fields struct {
//		Key     string
//		Value   []byte
//		Deleted bool
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   *Value
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			v := &Value{
//				Key:     tt.fields.Key,
//				Value:   tt.fields.Value,
//				Deleted: tt.fields.Deleted,
//			}
//			if got := v.copy(); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("copy() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
