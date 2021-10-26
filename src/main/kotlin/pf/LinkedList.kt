package pf

class LinkedList(_capacity: Int) {
    private val capacity = _capacity
    private val next = (0 .. capacity).toMutableList()
    private val prev = (0 .. capacity).toMutableList()

    /**
     * @brief 将 prevNode 和 nextNode 连起来
     * @param prevNode 前一个节点编号
     * @param nextNode 后一个节点编号
     */
    private fun link(nextNode: Int, prevNode: Int) {
        next[prevNode] = nextNode
        prev[nextNode] = prevNode
    }

    /**
     * @brief 从链表中删除一个节点
     * @param index 待删除节点编号
     */
    fun remove(index: Int) {
        if (prev[index] == index) // 这个位置本来就没在链表里
            return
        link(prev[index], next[index])
        // 把 index 之前和 index 之后连起来
        prev[index] = index
        next[index] = index
        // index 自己成环
    }

    /**
     * @brief 将一个节点添加到链表末尾（即头元素的 prev）
     * @param index 待添加节点编号
     */
    fun append(index: Int) {
        remove(index)
        link(prev[capacity], index)
        link(index, capacity)
    }

    /**
     * @brief 将一个节点添加到链表头
     * @param index 待添加节点编号
     */
    fun insertFirst(index: Int) {
        remove(index)
        link(capacity, index)
        link(index, next[capacity])
    }

    /**
     * @brief 获得链表的首元素
     * @return 首元素下标
     */
    fun getFirst(): Int {
        return next[capacity]
    }
}
