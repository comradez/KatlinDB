package pagedFile

class FindReplace(_capacity: Int) {
    private val capacity = _capacity
    private val list = LinkedList(capacity)
    init {
        for (i in capacity - 1 downTo 1) {
            list.insertFirst(i)
        }
    }

    /**
     * @brief 依据 LRU 分配下一个可用的 index
     * @return 下一个可用的 index
     * @remark 依据 LRU 原则，每次删除环状链表头部，并将新元素加到尾部
     */
    fun getIndex(): Int {
        val index = list.getFirst()
        list.remove(index)
        list.append(index)
        return index
    }

    /**
     * @brief 从缓冲区中释放一个 index 处的页面
     */
    fun free(index: Int) {
        list.insertFirst(index)
    }
}
