package bst

/**
 * @Author: ygzhang
 * @Date: 2023/12/26 20:36
 * @Func: 实现了一个堆栈stack
 **/

type Stack struct {
	stack  []*TreeNode
	bottom int // 栈底索引
	top    int //  栈顶索引
}

//
// NewStack
//  @Description: stack的构造函数, 初始化一个大小为n的栈
//  @param n
//  @return Stack
//
func NewStack(n int) Stack {
	return Stack{
		stack:  make([]*TreeNode, n),
		bottom: 0,
		top:    0,
	}
}

//
// Push
//  @Description: Push一个元素进栈
//  @receiver st
//  @param val
//
func (st *Stack) Push(val *TreeNode) {
	// 满了就append到切片里
	if st.Size() == st.Capacity() {
		st.stack = append(st.stack, val)
	} else {
		st.stack[st.top] = val
	}
	st.top++
}

func (st *Stack) Pop() (*TreeNode, bool) {
	// 如果栈空
	if st.Size() == 0 {
		return nil, false
	}
	st.top--
	return st.stack[st.top], true
}

func (st *Stack) Size() int {
	return st.top - st.bottom
}

func (st *Stack) Capacity() int {
	return len(st.stack)
}
