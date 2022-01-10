package indexManagement

import pagedFile.FileHandler
import utils.PAGE_SIZE
import utils.RID
import utils.readIntFromByteArray
import utils.writeIntToByteArray

/**
 * 使用 B+ 树维护的一个索引，每个 node 占一个 page.
 *
 * 其实不是 B+ 树...根本没重平衡
 */
class Index(
    private val fileHandler: FileHandler,
    _rootPageId: Int
) {
    private var root = InternalNode(_rootPageId, this.fileHandler)

    val rootPageId: Int
        get() = this.root.pageId

    fun load() = this.root.load()
    fun dump() = this.root.dump()
    fun debug() {
        this.root.debug()
    }

    fun put(key: Int, value: RID): RID? =
        when (this.root.children.isEmpty()) {
            false -> this.root.put(key, value).also {
                if (this.root.size > PAGE_SIZE) {
                    this.root = InternalNode(
                        this.fileHandler.freshPage(),
                        this.fileHandler,
                        _children = sequenceOf(this.root, this.root.split())
                            .map { child -> child.max to child }
                            .toMutableList(),
                        _dirty = true,
                        _subtreeDirty = true
                    )
                }
            }
            true -> {
                println("haha")
                this.root.dirty = true
                this.root.subtreeDirty = true
                val node = ExternalNode(
                    this.fileHandler.freshPage(),
                    this.fileHandler,
                    this.root.pageId,
                    _records = mutableListOf(key to value),
                    _dirty = true
                )
                this.root.children = mutableListOf(key to node)
                null
            }
        }

    fun remove(key: Int) = this.root.remove(key)

    fun get(key: Int) = this.root.get(key)

    /**
     * @return index 在 [[low], [high]] 内的 [RID]
     */
    fun get(low: Int, high: Int) = this.root.get(low, high)

    private abstract class TreeNode(
        val pageId: Int,
        val fileHandler: FileHandler,
        var parentPageId: Int,
        var dirty: Boolean
    ) {
        val isLeaf: Boolean
            get() = this is ExternalNode

        abstract val min: Int
        abstract val max: Int
        abstract val empty: Boolean
        abstract val size: Int
        abstract val bytes: ByteArray

        abstract fun put(key: Int, value: RID): RID?
        abstract fun remove(key: Int): RID?
        abstract fun get(key: Int): RID?
        abstract fun get(low: Int, high: Int): Sequence<RID>

        abstract fun load()
        open fun dump() {
            if (this.dirty) {
                this.fileHandler.updatePage(this.pageId, this.bytes)
            }
        }

        abstract fun debug()

        abstract fun split(): TreeNode

        companion object {
            fun load(nodePageId: Int, fileHandler: FileHandler): TreeNode {
                val buffer = fileHandler.readPage(nodePageId)
                return when (val type = readIntFromByteArray(buffer, 0)) {
                    0 -> InternalNode(nodePageId, fileHandler)
                    1 -> ExternalNode(nodePageId, fileHandler)
                    else -> error(type)
                }.also { it.load() }
            }
        }
    }

    /**
     * 存储结构：type = 0 | parent page ID | length | (key_i | children_i page ID)...
     *
     * key_i 为 children_i 的 max key
     */
    private class InternalNode(
        _pageId: Int,
        _fileHandler: FileHandler,
        _parentPageId: Int = -1,
        _children: MutableList<Pair<Int, TreeNode>> = mutableListOf(),
        _dirty: Boolean = false,
        _subtreeDirty: Boolean = false
    ) : TreeNode(_pageId, _fileHandler, _parentPageId, _dirty) {
        var children = _children // (key, child)
        var subtreeDirty = _subtreeDirty

        override val min: Int
            get() = this.children.first().first

        override val max: Int
            get() = this.children.last().first

        override val empty: Boolean
            get() = this.children.isEmpty()

        override val size: Int
            get() = 3 * Int.SIZE_BYTES + this.children.size * (2 * Int.SIZE_BYTES)

        override val bytes: ByteArray
            get() = ByteArray(PAGE_SIZE).also { buffer ->
                writeIntToByteArray(0, buffer, 0)
                writeIntToByteArray(this.parentPageId, buffer, Int.SIZE_BYTES)
                writeIntToByteArray(this.children.size, buffer, 2 * Int.SIZE_BYTES)
                this.children.forEachIndexed { i, (key, child) ->
                    writeIntToByteArray(key, buffer, (3 + 2 * i) * Int.SIZE_BYTES)
                    writeIntToByteArray(child.pageId, buffer, (3 + 2 * i + 1) * Int.SIZE_BYTES)
                }
            }

        override fun put(key: Int, value: RID): RID? {
            this.subtreeDirty = true
            val pos = when (val pos = this.children.binarySearchBy(key) { (key, _) -> key }) {
                in this.children.indices -> pos // key == key[pos]
                else -> when (val pos = -pos - 1) {
                    in this.children.indices -> pos // key[pos - 1] < key < key[pos]
                    else -> this.children.lastIndex.also { pos -> // max{key} < key
                        this.dirty = true
                        this.children[pos] = key to this.children[pos].second
                    }
                }
            }
            val (_, child) = this.children[pos]
            return child.put(key, value).also {
                if (child.size > PAGE_SIZE) {
                    this.dirty = true
                    val node = child.split()
                    this.children[pos] = child.max to child
                    this.children.add(pos + 1, node.max to node)
                }
            }
        }

        override fun remove(key: Int): RID? {
            this.subtreeDirty = true
            val pos = when (val pos = this.children.binarySearchBy(key) { (key, _) -> key }) {
                in this.children.indices -> pos.also { // key == key[pos]
                    this.dirty = true
                }
                else -> when (val pos = -pos - 1) {
                    in this.children.indices -> pos // key[pos - 1] < key < key[pos]
                    else -> return null // max{key} < key
                }
            }
            val (_, child) = this.children[pos]
            return child.remove(key).also {
                if (child.empty) {
                    this.dirty = true
                    this.children.removeAt(pos)
                } else {
                    this.children[pos] = child.max to child
                }
            }
        }

        override fun get(key: Int): RID? {
            val pos = when (val pos = this.children.binarySearchBy(key) { (key, _) -> key }) {
                in this.children.indices -> pos // key == key[pos]
                else -> when (val pos = -pos - 1) {
                    in this.children.indices -> pos // key[pos - 1] < key < key[pos]
                    else -> return null // max{key} < key
                }
            }
            return this.children[pos].second.get(key)
        }

        override fun get(low: Int, high: Int): Sequence<RID> {
            val l = when (val pos = this.children.binarySearchBy(low) { (key, _) -> key }) {
                in this.children.indices -> pos // low == key[pos]
                else -> when (val pos = -pos - 1) {
                    in this.children.indices -> pos // key[pos - 1] < low < key[pos]
                    else -> return emptySequence() // max{key} < low
                }
            }
            val r = when (val pos = this.children.binarySearchBy(high) { (key, _) -> key }) {
                in this.children.indices -> pos // high == key[pos]
                else -> when (val pos = -pos - 1) {
                    in this.children.indices -> pos // key[pos - 1] < high < key[pos]
                    else -> this.children.lastIndex // max{key} < high
                }
            }
            return this.children
                .slice(l..r).asSequence()
                .flatMap { (_, child) -> child.get(low, high) }
        }

        override fun load() {
            val buffer = this.fileHandler.readPage(this.pageId)
            assert(readIntFromByteArray(buffer, 0) == 0)
            this.parentPageId = readIntFromByteArray(buffer, Int.SIZE_BYTES)
            val length = readIntFromByteArray(buffer, 2 * Int.SIZE_BYTES)
            for (i in 0 until length) {
                val key = readIntFromByteArray(buffer, (3 + 2 * i) * Int.SIZE_BYTES)
                val childPageId = readIntFromByteArray(buffer, (3 + 2 * i + 1) * Int.SIZE_BYTES)
                val child = load(childPageId, this.fileHandler)
                this.children.add(key to child)
            }
        }

        override fun dump() {
            super.dump()
            if (this.subtreeDirty) {
                this.children.forEach { (_, child) -> child.dump() }
            }
        }

        override fun debug() {
            println("${this.pageId}: ${this.children.map { (key, child) -> "$key -> ${child.pageId}" }}")
            this.children.forEach { (_, child) -> child.debug() }
        }

        override fun split(): InternalNode {
            this.dirty = true
            val size = (this.children.size + 1) / 2
            val (l, r) = this.children.chunked(size).map { it.toMutableList() }
            this.children = l
            return InternalNode(
                fileHandler.freshPage(),
                fileHandler,
                parentPageId,
                r,
                true
            )
        }
    }

    /**
     * 存储结构：type = 1 | parent page ID | prev page ID | next page ID | length | (key_i | RID_i) ...
     */
    private class ExternalNode(
        _pageId: Int,
        _fileHandler: FileHandler,
        _parentPageId: Int = -1,
        _prevPageId: Int = -1,
        _nextPageId: Int = -1,
        _records: MutableList<Pair<Int, RID>> = mutableListOf(),
        _dirty: Boolean = false
    ) : TreeNode(_pageId, _fileHandler, _parentPageId, _dirty) {
        var prevPageId = _prevPageId
        var nextPageId = _nextPageId
        var records = _records // (key, RID)

        override val min: Int
            get() = this.records.first().first

        override val max: Int
            get() = this.records.last().first

        override val empty: Boolean
            get() = this.records.isEmpty()

        override val size: Int
            get() = 5 * Int.SIZE_BYTES + this.records.size * (3 * Int.SIZE_BYTES)

        override val bytes: ByteArray
            get() = ByteArray(PAGE_SIZE).also { buffer ->
                writeIntToByteArray(1, buffer, 0)
                writeIntToByteArray(this.parentPageId, buffer, Int.SIZE_BYTES)
                writeIntToByteArray(this.prevPageId, buffer, 2 * Int.SIZE_BYTES)
                writeIntToByteArray(this.nextPageId, buffer, 3 * Int.SIZE_BYTES)
                writeIntToByteArray(this.records.size, buffer, 4 * Int.SIZE_BYTES)
                this.records.forEachIndexed { i, (key, rid) ->
                    writeIntToByteArray(key, buffer, (5 + 3 * i) * Int.SIZE_BYTES)
                    writeIntToByteArray(rid.first, buffer, (5 + 3 * i + 1) * Int.SIZE_BYTES)
                    writeIntToByteArray(rid.second, buffer, (5 + 3 * i + 2) * Int.SIZE_BYTES)
                }
            }

        override fun put(key: Int, value: RID): RID? {
            this.dirty = true
            return when (val pos = this.records.binarySearchBy(key) { (key, _) -> key }) {
                in this.records.indices -> {
                    val (_, oldValue) = this.records[pos]
                    this.records[pos] = key to value
                    oldValue
                }
                else -> {
                    this.records.add(-pos - 1, key to value)
                    null
                }
            }
        }

        override fun remove(key: Int): RID? =
            when (val pos = this.records.binarySearchBy(key) { (key, _) -> key }) {
                in this.records.indices -> {
                    this.dirty = true
                    this.records.removeAt(pos).second
                }
                else -> null
            }

        override fun get(key: Int): RID? =
            when (val pos = this.records.binarySearchBy(key) { (key, _) -> key }) {
                in this.records.indices -> this.records[pos].second
                else -> null
            }

        override fun get(low: Int, high: Int): Sequence<RID> {
            val l = when (val pos = this.records.binarySearchBy(low) { (key, _) -> key }) {
                in this.records.indices -> pos
                else -> when (val pos = -pos - 1) {
                    in this.records.indices -> pos
                    else -> return emptySequence()
                }
            }
            val r = when (val pos = this.records.binarySearchBy(high) { (key, _) -> key }) {
                in this.records.indices -> pos
                else -> when (val pos = -pos - 2) {
                    in this.records.indices -> pos
                    else -> return emptySequence()
                }
            }
            return this.records.slice(l..r).asSequence().map { (_, rid) -> rid }
        }

        override fun load() {
            val buffer = this.fileHandler.readPage(this.pageId)
            assert(readIntFromByteArray(buffer, 0) == 1)
            this.parentPageId = readIntFromByteArray(buffer, Int.SIZE_BYTES)
            this.prevPageId = readIntFromByteArray(buffer, 2 * Int.SIZE_BYTES)
            this.nextPageId = readIntFromByteArray(buffer, 3 * Int.SIZE_BYTES)
            val length = readIntFromByteArray(buffer, 4 * Int.SIZE_BYTES)
            for (i in 0 until length) {
                val key = readIntFromByteArray(buffer, (5 + 3 * i) * Int.SIZE_BYTES)
                val rid = RID(
                    readIntFromByteArray(buffer, (5 + 3 * i + 1) * Int.SIZE_BYTES),
                    readIntFromByteArray(buffer, (5 + 3 * i + 2) * Int.SIZE_BYTES)
                )
                this.records.add(key to rid)
            }
        }

        override fun debug() {
            println("leaf ${this.pageId}: ${this.records.map { (key, _) -> key }}")
        }

        override fun split(): ExternalNode {
            this.dirty = true
            val size = (this.records.size + 1) / 2
            val (l, r) = this.records.chunked(size).map { it.toMutableList() }
            this.records = l
            val node = ExternalNode(
                fileHandler.freshPage(),
                fileHandler,
                this.parentPageId,
                this.pageId,
                this.nextPageId,
                r,
                true
            )
            this.nextPageId = node.pageId
            return node
        }
    }
}
