package indexManagement

import pagedFile.FileHandler
import utils.*

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

    fun put(key: Int, value: RID) {
        when (this.root.children.isEmpty()) {
            false -> {
                this.root.put(key, value)
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
            }
        }
    }

    fun remove(key: Int, value: RID) = this.root.remove(key, value)

    fun get(key: Int) = this.root.get(key)

    /**
     * @return index 在 [[low], [high]] 内的 [RID]
     */
    fun get(low: Int, high: Int): Sequence<RID> =
        if (low > high) {
            emptySequence()
        } else {
            this.root.get(low, high)
        }

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

        abstract fun put(key: Int, value: RID)
        abstract fun remove(key: Int, value: RID): Boolean
        abstract fun get(key: Int): Sequence<RID>
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

        override fun put(key: Int, value: RID) {
            this.subtreeDirty = true
            val pos = when (val pos = this.children.lowerBound(key) { (key, _) -> key }) {
                in this.children.indices -> pos
                else -> (pos - 1).also { pos ->
                    this.dirty = true
                    this.children[pos] = key to this.children[pos].second
                }
            }
            val (_, child) = this.children[pos]
            child.put(key, value)
            if (child.size > PAGE_SIZE) {
                this.dirty = true
                val node = child.split()
                this.children[pos] = child.max to child
                this.children.add(pos + 1, node.max to node)
            }
        }

        override fun remove(key: Int, value: RID): Boolean {
            val pos = this.children.lowerBound(key) { (key, _) -> key }
            for (i in pos until this.children.size) {
                val (max, child) = this.children[i]
                if (child.remove(key, value)) {
                    if (child.empty) {
                        this.dirty = true
                        this.children.removeAt(i)
                    } else {
                        this.subtreeDirty = true
                        if (max > child.max) {
                            this.dirty = true
                            this.children[i] = child.max to child
                        }
                    }
                    return true
                }
                if (max > key) {
                    break
                }
            }
            return false
        }

        override fun get(key: Int): Sequence<RID> = sequence {
            val pos = children.lowerBound(key) { (key, _) -> key }
            for (i in pos until children.size) {
                val (max, child) = children[i]
                yieldAll(child.get(key))
                if (max > key) {
                    break
                }
            }
        }

        override fun get(low: Int, high: Int): Sequence<RID> = sequence {
            val pos = children.lowerBound(low) { (key, _) -> key }
            for (i in pos until children.size) {
                val (max, child) = children[i]
                yieldAll(child.get(low, high))
                if (max > high) {
                    break
                }
            }
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

        override fun put(key: Int, value: RID) {
            this.dirty = true
            val pos = this.records.upperBound(key) { (key, _) -> key }
            this.records.add(pos, key to value)
        }

        override fun remove(key: Int, value: RID): Boolean {
            for (i in this.records.equalRange(key) { (key, _) -> key }) {
                if (this.records[i].second == value) {
                    this.records.removeAt(i)
                    return true
                }
            }
            return false
        }

        override fun get(key: Int): Sequence<RID> = sequence {
            for (i in records.equalRange(key) { (key, _) -> key }) {
                yield(records[i].second)
            }
        }

        override fun get(low: Int, high: Int): Sequence<RID> = sequence {
            val l = records.lowerBound(low) { (key, _) -> key }
            val r = records.upperBound(high) { (key, _) -> key }
            for (i in l until r) {
                yield(records[i].second)
            }
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
