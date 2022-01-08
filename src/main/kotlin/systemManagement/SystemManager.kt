package systemManagement

import indexManagement.IndexManager
import metaManagement.MetaHandler
import metaManagement.MetaManager
import metaManagement.info.ColumnDescription
import metaManagement.info.ColumnInfo
import metaManagement.info.TableInfo
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import pagedFile.BufferManager
import pagedFile.FileManager
import parser.*
import recordManagement.RecordHandler
import utils.*
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
    private val recordHandler = RecordHandler(this.bufferManager, this.workDir)
    private val indexManager = IndexManager(this.bufferManager, this.workDir)
    private val metaManager = MetaManager(this.fileManager, this.workDir)
    private val visitor = DatabaseVisitor(this)
    private val databases = File(this.workDir)
        .list { dir, _ -> dir.isDirectory }
        .orEmpty()
        .toMutableSet()

    var selectedDatabase: String? = null
    private val selectedDatabaseMeta: MetaHandler
        get() = this.metaManager.openMeta(this.selectedDatabase ?: throw NoUsingDatabaseError())

    private fun databasePath(databaseName: String): String = "${this.workDir}/${databaseName}"

    private fun selectTable(tableName: String): Pair<MetaHandler, TableInfo> {
        val metaHandler = this.selectedDatabaseMeta
        if (tableName !in metaHandler.dbInfo.tableMap) {
            throw TableNotExistsError(tableName)
        }
        val tableInfo = metaHandler.getTable(tableName)
        return Pair(metaHandler, tableInfo)
    }

    private fun selectColumn(
        tableName: String,
        columnName: String
    ): Triple<MetaHandler, TableInfo, Int> {
        val (metaHandler, tableInfo) = this.selectTable(tableName)
        if (columnName !in tableInfo.columnMap) {
            throw ColumnNotExistsError(tableName, columnName)
        }
        val columnIndex = tableInfo.getColumnIndex(columnName)
        return Triple(metaHandler, tableInfo, columnIndex)
    }

    fun execute(sql: String): List<QueryResult> {
        val stream = CharStreams.fromString(sql)
        val lexer = SQLLexer(stream)
        val tokens = CommonTokenStream(lexer)
        val parser = SQLParser(tokens)
        val tree = parser.program()
        return (this.visitor.visit(tree) as List<*>).map { it as QueryResult }
    }

    fun load(filename: String, tableName: String) {
        TODO()
    }

    fun dump(filename: String, tableName: String) {
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
        this.recordHandler.closeDatabase(databaseName)
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
        val metaHandler = this.selectedDatabaseMeta
        if (info.name in metaHandler.dbInfo.tableMap) {
            throw TableAlreadyExistsError(info.name)
        }
        metaHandler.addTable(info)
        this.recordHandler.createRecord(metaHandler.dbName, info.name, info.totalSize)
    }

    fun describeTable(tableName: String): QueryResult {
        val (_, tableInfo) = this.selectTable(tableName)
        val info = tableInfo.describe()
        return SuccessResult(ColumnDescription.keys, info.map { it.values })
    }

    fun renameTable(oldName: String, newName: String) {
        val (metaHandler, _) = this.selectTable(oldName)
        if (newName in metaHandler.dbInfo.tableMap) {
            throw TableAlreadyExistsError(newName)
        }
        metaHandler.renameTable(oldName, newName)
        this.recordHandler.renameRecord(metaHandler.dbName, oldName, newName)
    }

    fun dropTable(tableName: String) {
        val (metaHandler, _) = this.selectTable(tableName)
        metaHandler.removeTable(tableName)
        this.recordHandler.removeRecord(metaHandler.dbName, tableName)
    }

    fun addColumn(tableName: String, info: ColumnInfo) {
        val (metaHandler, newTableInfo) = this.selectTable(tableName)
        val oldTableInfo = TableInfo(newTableInfo.name, newTableInfo.columns.toList()) // deep copy
        if (info.name in oldTableInfo.columnMap) {
            throw ColumnAlreadyExistsError(tableName, info.name)
        }
        metaHandler.addColumn(tableName, info)
        val newRecord = this.recordHandler.createRecord(
            metaHandler.dbName,
            "${tableName}.copy",
            newTableInfo.totalSize
        )
        val oldRecord = this.recordHandler.openRecord(metaHandler.dbName, tableName)
        oldRecord.getItemRIDs()
            .map { oldTableInfo.parseRecord(oldRecord.getRecord(it)) }
            .forEach { values ->
                values.toMutableList().add(info.getDescription().default)
                val record = newTableInfo.buildRecord(values)
                newRecord.insertRecord(record)
            }
        this.recordHandler.removeRecord(metaHandler.dbName, tableName)
        this.recordHandler.renameRecord(metaHandler.dbName, "${tableName}.copy", tableName)
    }

    fun dropColumn(tableName: String, columnName: String) {
        val (metaHandler, newTableInfo, columnIndex) = this.selectColumn(tableName, columnName)
        val oldTableInfo = TableInfo(newTableInfo.name, newTableInfo.columns.toList()) // deep copy
        val newRecord = this.recordHandler.createRecord(
            metaHandler.dbName,
            "${tableName}.copy",
            newTableInfo.totalSize
        )
        val oldRecord = this.recordHandler.openRecord(metaHandler.dbName, tableName)
        oldRecord.getItemRIDs()
            .map { oldTableInfo.parseRecord(oldRecord.getRecord(it)) }
            .forEach { values ->
                values.toMutableList().removeAt(columnIndex)
                val record = newTableInfo.buildRecord(values)
                newRecord.insertRecord(record)
            }
        this.recordHandler.removeRecord(metaHandler.dbName, tableName)
        this.recordHandler.renameRecord(metaHandler.dbName, "${tableName}.copy", tableName)
    }

    fun insertRecord(tableName: String, values: List<Any?>) {
        val (metaHandler, tableInfo) = this.selectTable(tableName)
        val row = tableInfo.buildRecord(values) // TODO 合法性检查
        this.checkConstraints(tableInfo, values)
        val record = this.recordHandler.openRecord(metaHandler.dbName, tableName)
        val rid = record.insertRecord(row)
        this.insertIndex(metaHandler, tableInfo, rid, values)
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
        val (metaHandler, tableInfo, _) = this.selectColumn(tableName, columnName)
        metaHandler.addUnique(tableName, columnName)
        if (!tableInfo.existIndex(columnName)) {
            this.createIndex(tableName, columnName)
        }
    }

    fun setPrimary(tableName: String, primary: List<String>) {
        val (metaHandler, tableInfo) = this.selectTable(tableName)
        primary.firstOrNull { it !in tableInfo.columnMap }?.let {
            throw ColumnNotExistsError(tableName, it)
        }
        metaHandler.setPrimary(tableName, primary)
        primary.asSequence()
            .filterNot { tableInfo.existIndex(it) }
            .forEach { this.createIndex(tableName, it) }
    }

    fun dropPrimary(tableName: String) {
        val (metaHandler, tableInfo) = this.selectTable(tableName)
        metaHandler.setPrimary(tableName, listOf())
        tableInfo.primary.asSequence()
            .filter { tableInfo.existIndex(it) }
            .forEach { this.dropIndex(tableName, it) }
    }

    fun addForeign(tableName: String, columnName: String, foreign: Pair<String, String>) {
        val (foreignTableName, foreignColumnName) = foreign
        val (metaHandler, foreignTableInfo) = this.selectColumn(foreignTableName, foreignColumnName)
        if (columnName in metaHandler.dbInfo.tableMap) {
            throw ColumnAlreadyIndexedError(tableName, columnName)
        }
        metaHandler.addForeign(tableName, columnName, foreign)
        if (!foreignTableInfo.existIndex(foreignColumnName)) {
            this.createIndex(foreignTableName, foreignColumnName)
        }
    }

    fun dropForeign(tableName: String, columnName: String) {
        val (metaHandler, tableInfo, _) = this.selectColumn(tableName, columnName)
        val (foreignTableName, foreignColumnName) = tableInfo.foreign[columnName]
            ?: throw NotForeignKeyColumnError(tableName, columnName)
        metaHandler.removeForeign(tableName, columnName)
        this.dropIndex(foreignTableName, foreignColumnName)
    }

    fun showIndices(): List<String> {
        val metaHandler = this.selectedDatabaseMeta
        return metaHandler.dbInfo.indexMap.keys.toList()
    }

    fun createIndex(
        tableName: String,
        columnName: String,
        indexName: String = "${tableName}.${columnName}"
    ) {
        val (metaHandler, tableInfo, _) = this.selectColumn(tableName, columnName)
        if (metaHandler.existIndex(indexName)) {
            throw IndexAlreadyExistsError(indexName)
        }
        if (tableInfo.existIndex(columnName)) {
            throw ColumnAlreadyIndexedError(tableName, columnName)
        }
        metaHandler.createIndex(indexName, tableName, columnName)
        val index = this.indexManager.createIndex(metaHandler.dbName, tableName, indexName)
        tableInfo.createIndex(columnName, index.rootPageId)
        val columnIndex = tableInfo.getColumnIndex(columnName)
        val record = this.recordHandler.openRecord(metaHandler.dbName, tableName)
        record.getItemRIDs().forEach { rid ->
            val values = tableInfo.parseRecord(record.getRecord(rid))
            val key = values[columnIndex] as Int
            index.put(key, rid)
        }
    }

    fun renameIndex(oldName: String, newName: String) {
        val metaHandler = this.selectedDatabaseMeta
        if (!metaHandler.existIndex(oldName)) {
            throw IndexNotExistsError(oldName)
        }
        if (metaHandler.existIndex(newName)) {
            throw IndexAlreadyExistsError(newName)
        }
        val (tableName, _) = metaHandler.getIndexInfo(oldName)
        val tableInfo = metaHandler.getTable(tableName)
        tableInfo.renameIndex(oldName, newName)
        metaHandler.renameIndex(oldName, newName)
    }

    fun dropIndex(tableName: String, columnName: String) {
        this.selectColumn(tableName, columnName)
        this.dropIndex("${tableName}.${columnName}")
    }

    fun dropIndex(indexName: String) {
        val metaHandler = this.selectedDatabaseMeta
        if (!metaHandler.existIndex(indexName)) {
            throw IndexNotExistsError(indexName)
        }
        val (tableName, _) = metaHandler.getIndexInfo(indexName)
        val tableInfo = metaHandler.getTable(tableName)
        tableInfo.dropIndex(indexName)
        metaHandler.dropIndex(indexName)
    }

    fun resultToValue(result: QueryResult, isIn: Boolean): Any {
        TODO()
    }

    private fun checkConstraints(tableInfo: TableInfo, values: List<Any?>) {
        // TODO
    }

    private fun insertIndex(
        metaHandler: MetaHandler,
        tableInfo: TableInfo,
        rid: RID,
        values: List<Any?>
    ) {
        TODO()
    }

    private fun selectIndices(
        tableInfo: TableInfo,
        conditions: List<Condition>
    ): Sequence<RID> {
        val ranges = mutableMapOf<String, Pair<Int, Int>>()
        TODO()
//        conditions.asSequence()
//            .filter { condition -> condition.tableName == tableInfo.name }
//            .filter { condition -> tableInfo.existIndex(condition.columnName) }
//            .forEach { (_, columnName, predicate) ->
//                if ()
//            }
    }

    private fun selectRecords(
        tableInfo: TableInfo,
        predicate: Predicate
    ): Sequence<Pair<RID, Any?>> = sequence {
        TODO()
    }
}
