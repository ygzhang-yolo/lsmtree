# lsm
一个lsm Tree的go语言简单实现

# quick start
```go

import 	lsm "github.com/ygzhang-yolo/lsmtree"

lsm.Start(config.Config{
  DataDir:       `D:\study\杂项\lsmData`,
  Level0Size:    100,
  PartSize:      4,
  Threshold:     10000,
  CheckInterval: 3,
})

lsm.Set[string]("aaa", "aaa_value")
v, _ := lsm.Get[string]("aaa")
fmt.Println(v)
```

# test
example中提供了四种test:
1. 基本读写功能：basicGetAndSet();
2. 测试wal：crashWithWal();
3. 测试SSTable: writeSSTable();
4. 测试压缩：SSTableCompaction();
