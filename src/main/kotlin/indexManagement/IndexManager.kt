package indexManagement

import pagedFile.BufferManager
import pagedFile.FileHandler

private typealias Table = MutableMap<String, Index> // indexName => index
private typealias Database = Pair<FileHandler, MutableMap<String, Table>> // tableName => table

/**
 * 向上层暴露的管理索引的接口。
 */
class IndexManager(
    _bufferManager: BufferManager,
    _workDir: String
) {
    private val bufferManager = _bufferManager
    private val workDir = _workDir
    private val databases = mutableMapOf<String, Database>()

    fun createIndex(databaseName: String, tableName: String, indexName: String): Index {
        val (handler, tables) = this.getDatabase(databaseName)
        return Index(handler, handler.freshPage()).also { index ->
            tables[tableName]!![indexName] = index
        }
    }

    fun openIndex(
        databaseName: String,
        tableName: String,
        indexName: String,
        rootPageId: Int
    ): Index {
        val (handler, tables) = this.getDatabase(databaseName)
        return tables[tableName]!!.getOrPut(indexName) {
            Index(handler, rootPageId).also { it.load() }
        }
    }

    fun closeIndex(databaseName: String, tableName: String, indexName: String) {
        val (_, tables) = this.getDatabase(databaseName)
        tables[tableName]!!.remove(indexName)?.dump()
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
