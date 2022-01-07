package systemManagement

import indexManagement.IndexManager
import metaManagement.MetaManager
import metaManagement.info.ColumnInfo
import metaManagement.info.TableInfo
import pagedFile.BufferManager
import pagedFile.FileManager
import parser.DatabaseVisitor
import parser.QueryResult
import recordManagement.RecordHandler
import utils.DatabaseAlreadyExistsError
import utils.DatabaseNotExistsError
import utils.NoUsingDatabaseError
import java.io.File

class SystemManager(private val workDir: String) {
    init {
        with(File(workDir)) {
            this.mkdirs()
            assert(this.isDirectory)
        }
    }

    private val fileManager = FileManager()
    private val bufferManager = BufferManager(this.fileManager)
    private val recordHandler = RecordHandler(this.bufferManager)
    private val indexManager = IndexManager(this.bufferManager, this.workDir)
    private val metaManager = MetaManager(this.fileManager)
    private val visitor = DatabaseVisitor(this)
    private val databases = File(this.workDir)
        .list { dir, _ -> dir.isDirectory }
        .orEmpty()
        .toMutableSet()

    private var selectedDatabase: String? = null

    private fun databasePath(databaseName: String): String = "${this.workDir}/${databaseName}"

    fun execute(sql: String): QueryResult {
        TODO()
    }

    fun showDatabases(): List<String> = this.databases.toList()

    fun createDatabase(databaseName: String) {
        if (databaseName in this.databases) {
            throw DatabaseAlreadyExistsError(databaseName)
        }
        with(File(this.databasePath(databaseName))) {
            this.mkdirs()
            assert(this.isDirectory)
        }
        this.databases.add(databaseName)
    }

    /**
     * @return true，若 [databaseName] 正在使用
     */
    fun dropDatabase(databaseName: String): Boolean {
        if (databaseName !in this.databases) {
            throw DatabaseNotExistsError(databaseName)
        }
//        File(this.databasePath(databaseName))
//            .listFiles { file -> file.extension == "table" }
//            ?.forEach { table ->
//                this.recordHandler.closeFile()
//            }
        this.indexManager.closeDatabase(databaseName)
        this.metaManager.closeMeta(databaseName)
        File(this.databasePath(databaseName)).deleteRecursively()
        return if (this.selectedDatabase == databaseName) {
            this.selectedDatabase = null
            true
        } else {
            false
        }
    }

    /**
     * @return 当前使用的数据库名
     */
    fun useDatabase(databaseName: String): String {
        if (databaseName !in this.databases) {
            throw DatabaseNotExistsError(databaseName)
        }
        return databaseName.also { this.selectedDatabase = it }
    }

    fun showTables(): List<String> =
        File(this.databasePath(this.selectedDatabase ?: throw NoUsingDatabaseError()))
            .list { file, _ -> file.extension == "table" }
            .orEmpty()
            .toList()

    fun createTable(info: TableInfo) {
        TODO()
    }

    fun describeTable(tableName: String): QueryResult {
        TODO()
    }

    fun renameTable(oldName: String, newName: String) {
        TODO()
    }

    fun dropTable(tableName: String) {
        TODO()
    }

    fun addColumn(tableName: String, info: ColumnInfo) {
        TODO()
    }

    fun dropColumn(tableName: String, columnName: String) {
        TODO()
    }

    fun <T> insertRecord(tableName: String, values: List<T>) {
        TODO()
    }

    fun selectRecords(
        selectors: List<Selector>,
        tableNames: List<String>,
        conditions: List<Condition> = listOf(),
        groupBy: Pair<String, String>? = null,
        limit: Int = -1,
        offset: Int = -1
    ): QueryResult {
        TODO()
    }

    fun updateRecords(
        tableName: String,
        conditions: List<Condition>,
        values: Map<String, Any?>
    ): QueryResult {
        TODO()
    }

    fun deleteRecords(tableName: String, conditions: List<Condition>): QueryResult {
        TODO()
    }

    fun addUnique(tableName: String, columnName: String) {
        TODO()
    }

    fun setPrimary(tableName: String, primary: List<String>?) {
        TODO()
    }

    fun dropPrimary(tableName: String) {
        TODO()
    }

    fun addForeign(
        tableName: String,
        column: ColumnInfo,
        foreign: Pair<String, String>,
        foreignName: String? = null
    ) {
        TODO()
    }

    fun dropForeign(tableName: String, column: ColumnInfo, foreignName: String?) {
        TODO()
    }

    fun showIndices(): List<String> {
        TODO()
    }

    fun createIndex(tableName: String, columnName: String, indexName: String) {
        TODO()
    }

    fun renameIndex(oldName: String, newName: String) {
        TODO()
    }

    fun dropIndex(indexName: String) {
        TODO()
    }

    fun resultToValue(result: QueryResult, isIn: Boolean): Any {
        TODO()
    }
}
