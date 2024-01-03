package bst

import (
	"github.com/ygzhang-yolo/lsmtree/kv"
	"log"
	"sync"
)

/**
 * @Author: ygzhang
 * @Date: 2023/12/26 19:25
 * @Func: 实现二叉搜索树bst
 **/

//
//  treeNode
//  @Description: treeNode树节点
//
type TreeNode struct {
	KV    kv.Value
	Left  *TreeNode
	Right *TreeNode
}

//
//  BSTree
//  @Description: 二叉搜索树
//
type BSTree struct {
	root  *TreeNode
	count int
	mu    *sync.RWMutex
}

func NewBSTree() BSTree {
	return BSTree{
		root:  nil,
		count: 0,
		mu:    &sync.RWMutex{},
	}
}

//
// GetCount
//  @Description: 返回BST树中的元素数量
//  @receiver t
//  @return int
//
func (t *BSTree) GetCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.count
}

//
// Get
//  @Description: 在BST中二分查找键为key的节点, 返回key的值value
//  @receiver t
//  @param key
//  @return kv.Value
//  @return kv.SearchResult
//
func (t *BSTree) Get(key string) (kv.Value, kv.SearchResult) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// tree必须非空
	if t == nil {
		log.Fatalf("Get [key=%v] error, tree is nil!", key)
	}
	// BST的二分搜索
	cur := t.root
	for cur != nil {
		if key > cur.KV.Key {
			cur = cur.Right
		} else if key < cur.KV.Key {
			cur = cur.Left
		} else {
			// 找到了对应的节点
			// 确定是否是删掉的节点
			if cur.KV.Deleted == true {
				return kv.Value{}, kv.Deleted
			} else {
				return cur.KV, kv.Success
			}
		}
	}
	// 否则，说明找不到
	return kv.Value{}, kv.None
}

//
// Set
//  @Description: Set 设置key的值为value并返回旧值
//  @receiver t
//  @param key
//  @param value
//  @return old	旧值
//  @return valid	是否有旧值
//
func (t *BSTree) Set(key string, value []byte) (kv.Value, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// tree必须非空
	if t == nil {
		log.Fatalf("Set [key=%v] error, tree is nil!", key)
	}

	newNode := &TreeNode{
		KV: kv.Value{
			Key:   key,
			Value: value,
		},
	}
	cur := t.root
	// 如果当前还没有根节点, 则新建并返回
	if cur == nil {
		t.root = newNode
		t.count++
		return kv.Value{}, false
	}
	// 二分查找key的位置
	for cur != nil {
		if key < cur.KV.Key {
			// 要插入左子树
			if cur.Left == nil {
				// 左子树为空, 可直接插入
				cur.Left = newNode
				t.count++
				return kv.Value{}, false
			}
			cur = cur.Left
		} else if key > cur.KV.Key {
			// 要插入右子树
			if cur.Right == nil {
				// 右子树为空, 直接插入
				cur.Right = newNode
				t.count++
				return kv.Value{}, false
			}
			cur = cur.Right
		} else {
			// 树里key已经存在, 替换新值并返回旧值
			old := cur.KV.Copy()
			cur.KV.Value = value
			cur.KV.Deleted = false
			if old.Deleted {
				return kv.Value{}, false
			} else {
				return *old, true
			}
		}
	}
	// 说明插入树中失败
	log.Fatalf("Set value error, [key=%v, value=%v]]", key, value)
	return kv.Value{}, false
}

//
// Delete
//  @Description: 删除key并返回旧值
//  @receiver t
//  @param key
//  @return kv.Value
//  @return bool
//
func (t *BSTree) Delete(key string) (kv.Value, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// tree必须非空
	if t == nil {
		log.Fatalf("Delete [key=%v] error, tree is nil!", key)
	}
	del := &TreeNode{
		KV: kv.Value{
			Key:     key,
			Value:   nil,
			Deleted: true,
		},
	}
	cur := t.root
	if cur == nil {
		t.root = del
		return kv.Value{}, false
	}
	// 二分查找
	for cur != nil {
		if key < cur.KV.Key {
			if cur.Left == nil {
				// 删除不存在的key, 将对应的delete位置true
				cur.Left = del
				t.count++
			}
			cur = cur.Left
		} else if key > cur.KV.Key {
			if cur.Right == nil {
				cur.Right = del
				t.count++
			}
			cur = cur.Right
		} else {
			// 存在的key, 判断是否已经删除
			if cur.KV.Deleted == true {
				// 已经删除了就直接返回
				return kv.Value{}, false
			} else {
				old := cur.KV.Copy()
				cur.KV = del.KV
				// NOTE: count 应该是统计当前树中存在的有效节点，但是如果删除一个不存在的key，这个count会计算错误, 应该要在添加删除Node的时候count增加一下来保证count数量正确
				t.count--
				return *old, true
			}
		}
	}
	log.Fatalf("Delete value error, [key=%v]]", key)
	return kv.Value{}, false
}

//
// GetKV
//  @Description: 中序遍历返回树中的所有元素
//  @receiver t
//  @return []kv.Value	返回一个有序的元素切片
//
func (t *BSTree) GetKV() []kv.Value {
	//中序遍历得到有序列表
	t.mu.RLock()
	defer t.mu.RUnlock()
	st := NewStack(t.count)
	values := make([]kv.Value, 0)

	// 迭代实现中序遍历
	cur := t.root
	for {
		if cur != nil {
			st.Push(cur)
			cur = cur.Left
		} else {
			node, ok := st.Pop()
			if !ok {
				break
			}
			values = append(values, node.KV)
			cur = node.Right
		}
	}
	return values
}

//
// Swap
//  @Description: 将t置空, 返回t的副本
//  @receiver t
//
func (t *BSTree) Swap() *BSTree {
	t.mu.Lock()
	defer t.mu.Unlock()

	newTree := NewBSTree()
	newTree.root = t.root
	newTree.count = t.count
	t.root = nil
	t.count = 0
	return &newTree
}
