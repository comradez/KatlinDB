package utils

class UnionFindSet<T>(_set: Set<T>) : Map<T, T> {
    private val setMap = _set.asSequence().map { it to it }.toMap().toMutableMap()
    private val rankMap = _set.asSequence().map { it to 0 }.toMap().toMutableMap()

    override val entries get() = this.setMap.entries
    override val keys get() = this.setMap.keys
    override val values get() = this.setMap.values
    override val size get() = this.setMap.size

    override fun isEmpty(): Boolean = this.setMap.isEmpty()

    override fun containsKey(key: T): Boolean = key in this.keys

    override fun containsValue(value: T): Boolean = value in this.values

    override fun get(key: T): T? =
        if (key in this) {
            this.find(key)
        } else {
            null
        }

    fun union(_x: T, _y: T): T {
        var x = this.find(_x)
        var y = this.find(_y)
        val rankX = this.rankMap[x]!!
        val rankY = this.rankMap[y]!!
        if (rankX < rankY) {
            val (x_, y_) = Pair(y, x)
            x = x_; y = y_
        } else if (rankX == rankY) {
            this.rankMap[x] = rankX + 1
        }
        this.setMap[y] = x
        return x
    }

    fun find(key: T): T {
        var belong: T = this.setMap[key]!!
        if (key != belong) {
            belong = this.find(belong)
            this.setMap[key] = belong
        }
        return belong
    }
}
