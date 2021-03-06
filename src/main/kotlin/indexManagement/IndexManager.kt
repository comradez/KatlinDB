package indexManagement

import pagedFile.BufferManager
import pagedFile.FileHandler

private typealias Table = MutableMap<String, Index> // columnName => index
private typealias Database = Pair<FileHandler, MutableMap<String, Table>> // tableName => table

/**
 * 向上层暴露的管理索引的接口。
 */
class IndexManager(
    _bufferManager: BufferManager,
    _workDir: String
) : AutoCloseable {
    private val bufferManager = _bufferManager
    private val workDir = _workDir
    private val databases = mutableMapOf<String, Database>()

    override fun close() {
        val databases = this.databases.keys.toList()
        databases.forEach { database -> this.closeDatabase(database) }
    }

    fun createIndex(databaseName: String, tableName: String, columnName: String): Index {
        val (handler, tables) = this.getDatabase(databaseName)
        return Index(handler, handler.freshPage(), false).also { index ->
            tables.getOrPut(tableName) { mutableMapOf() }[columnName] = index
        }
    }

    fun openIndex(
        databaseName: String,
        tableName: String,
        columnName: String,
        rootPageId: Int
    ): Index {
        val (handler, tables) = this.getDatabase(databaseName)
        return tables.getOrPut(tableName) { mutableMapOf() }
            .getOrPut(columnName) {
                Index(handler, rootPageId, false).also { it.load() }
            }//.also { it.debug() }
    }

    fun closeIndex(databaseName: String, tableName: String, columnName: String) {
        val (_, tables) = this.getDatabase(databaseName)
        tables[tableName]!!.remove(columnName)?.dump()
    }

    fun closeDatabase(databaseName: String) {
        this.databases.remove(databaseName)?.let { (handler, tables) ->
            tables.values.asSequence()
                .flatMap { table -> table.values }
                .forEach { index -> index.dump() }
            handler.close()
        }
    }

    private fun getDatabase(databaseName: String): Database =
        this.databases.getOrPut(databaseName) {
            val handler = FileHandler(
                this.bufferManager,
                "${workDir}/${databaseName}/${databaseName}.index"
            )
            Pair(handler, mutableMapOf())
        }
}
