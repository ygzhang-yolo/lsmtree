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

其中, config代表lsm的配置：
- DataDir：wal和db文件存储的路径;
- Level0Size：level0层的所有SSTable文件大小总和的最大值(MB);
- PartSize: 每层中SSTable表的数量限制
- Threshold: 内存表中kv的数量限制；
- CheckInterval: 内存, SSTable压缩检查的时间间隔;


# test
example中提供了四种test:
1. 基本读写功能：basicGetAndSet();
2. 测试wal：crashWithWal();
3. 测试SSTable: writeSSTable();
4. 测试压缩：SSTableCompaction();

# dir
- bst: 内存表相关, 以二叉搜索树BST的形式组织;
- config: lsm的配置config相关;
- db: lsm对外提供的数据库存储database;
- example: 提供的一些测试用例;
- kv: 底层的key-value存储
- monitor: 后台监视内存和SSTable压缩相关
- ssTable: SSTable结构;
- sstTree: SSTable组织成的树的形式;
- wal: wal相关
