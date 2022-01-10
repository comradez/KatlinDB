package systemManagement

import dataConverter.Converter
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
import recordManagement.AttributeType
import recordManagement.CompareOp
import recordManagement.RecordHandler
import utils.*
import java.io.File
import java.io.FileWriter
import java.util.*
import kotlin.math.max
import kotlin.math.min

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
    private val databases get() =  File(this.workDir)
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

    private fun columnNameResolution(columns: Sequence<UnqualifiedColumn>, tableInfo: TableInfo) {
        columns.forEach { column ->
            if (column.tableName == null) {
                column.tableName = tableInfo.name
            } else if (column.tableName != tableInfo.name) {
                throw BadColumnIdentifier("Column `${column}` mismatched with table `${tableInfo.name}`")
            }
        }
    }

    private fun columnNameResolution(
        columns: Sequence<UnqualifiedColumn>,
        tableInfos: Sequence<TableInfo>
    ) {
        val tables = tableInfos.map { it.name }.toSet()
        val candidates = mutableMapOf<String, MutableSet<String>>()
        tableInfos.forEach { tableInfo ->
            tableInfo.columns.forEach { columnInfo ->
                candidates.getOrPut(columnInfo.name) { mutableSetOf() }
                    .add(tableInfo.name)
            }
        }
        columns.forEach { column ->
            if (column.tableName == null) {
                val tableNames = candidates[column.columnName]
                    ?: throw BadColumnIdentifier("Unknown column `${column}`")
                column.tableName = tableNames.singleOrNull()
                    ?: throw BadColumnIdentifier("Column `${column}` is ambiguous")
            } else if (column.tableName !in tables) {
                throw BadColumnIdentifier("Unknown column `${column}`")
            }
        }
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
        val file = File(filename)
        val (_, tableInfo) = this.selectTable(tableName)
        val types = tableInfo.columnIndices.map { (_, columnIndex) ->
            tableInfo.columns[columnIndex].type
        }
        if (file.exists() && file.isFile) {
            val lines = file.readLines()
            for (line in lines) {
                val pieces = line.split(',')
                val dataForInsert = (pieces zip types).map { (piece, type) ->
                    Converter.convertFromString(
                        piece,
                        type
                    )
                }
                insertRecord(tableName, dataForInsert)
            }
        } else {
            throw FileNotExistError(filename)
        }
    }

    fun dump(filename: String, tableName: String) {
        val file = File(filename)
        val (metaHandler, tableInfo) = this.selectTable(tableName)
        val types = tableInfo.columnIndices.map { (_, columnIndex) ->
            tableInfo.columns[columnIndex].type
        }
        if (!file.exists()) {
            file.createNewFile()
            val writer = FileWriter(filename, true)
            val records = selectRecords(metaHandler, tableInfo, listOf())
            for ((_, record) in records) {
                val dataForDump = (record zip types).joinToString(",") { (data, type) ->
                    Converter.convertToString(
                        data,
                        type
                    )
                }
                writer.write(dataForDump + "\n")
            }
            writer.close()
        } else {
            throw FileAlreadyExistsError(filename)
        }
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
            .list { _, name -> name.substringAfterLast('.', "") == "table" }
            .orEmpty()
            .toList()
            .map { it.substringBeforeLast('.', "") }
            .also { println("table num: ${it.size}") }

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
            .forEach { row ->
                row.toMutableList().add(info.getDescription().default)
                val record = newTableInfo.buildRecord(row)
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
            .forEach { row ->
                row.toMutableList().removeAt(columnIndex)
                val record = newTableInfo.buildRecord(row)
                newRecord.insertRecord(record)
            }
        this.recordHandler.removeRecord(metaHandler.dbName, tableName)
        this.recordHandler.renameRecord(metaHandler.dbName, "${tableName}.copy", tableName)
    }

    fun insertRecord(tableName: String, row: List<Any?>) {
        val (metaHandler, tableInfo) = this.selectTable(tableName)
        this.checkInsertConstraints(metaHandler, tableInfo, row)
        val record = this.recordHandler.openRecord(metaHandler.dbName, tableName)
        val rid = record.insertRecord(tableInfo.buildRecord(row))
        this.insertIndex(metaHandler, tableInfo, rid, row)
    }

    fun selectRecords(
        selectors: List<Selector>,
        tableNames: Set<String>,
        conditions: List<Condition> = listOf(),
        groupBy: UnqualifiedColumn? = null,
        limit: Int? = null,
        offset: Int? = null
    ): QueryResult {
        assert(selectors.isNotEmpty())
        val metaHandler = this.selectedDatabaseMeta
        val tableInfos = tableNames.map { tableName ->
            metaHandler.dbInfo.tableMap[tableName] ?: throw TableNotExistsError(tableName)
        }
        this.columnNameResolution(
            selectors.asSequence().filterIsInstance<AggregationSelector>().map { it.column } +
                    selectors.asSequence().filterIsInstance<FieldSelector>().map { it.column } +
                    conditions.asSequence().flatMap {
                        when (it) {
                            is JoinCondition -> sequenceOf(it.column, it.targetColumn)
                            else -> sequenceOf(it.column)
                        }
                    } + sequence { groupBy?.also { yield(it) } },
            tableInfos.asSequence()
        )
        if (groupBy == null &&
            selectors.any { it is FieldSelector || it is WildcardSelector } &&
            selectors.any { it is AggregationSelector || it is WildcardCountSelector }
        ) {
            throw IllFormSelectError("SELECT without GROUP BY should not contain both field and aggregation selectors")
        }
        // just a trick
        if (selectors.singleOrNull() is WildcardCountSelector &&
            groupBy == null &&
            tableNames.size == 1
        ) {
            val record = this.recordHandler.openRecord(metaHandler.dbName, tableNames.single())
            return SuccessResult(listOf("Count"), listOf(listOf(record.config.recordNumber)))
        }
        val predicateConditions = conditions.filterIsInstance<PredicateCondition>()
        val joinConditions = conditions.filterIsInstance<JoinCondition>()
        val results = tableNames.associateWith { tableName ->
            val tableInfo = metaHandler.getTable(tableName)
            val record = this.recordHandler.openRecord(metaHandler.dbName, tableName)
            val data = this.selectIndices(metaHandler, tableInfo, predicateConditions)
                .map { rid -> tableInfo.parseRecord(record.getRecord(rid)) }
            SuccessResult(tableInfo.getHeader(), data.toList())
        }
        val result = results.values.singleOrNull()
            ?: this.joinEquals(results, joinConditions)
        if (groupBy != null) {
            when (selectors.first()) {
                is WildcardSelector -> {
                    if (result.columnSize > 1) {
                        throw IllFormSelectError(
                            "Cannot GROUP ${
                                trimListToPrint(
                                    result.headers,
                                    3
                                )
                            } BY $groupBy"
                        )
                    }
                    val data = result.data.map { it.single() }.distinct()
                    return SuccessResult(result.headers, data.map { listOf(it) })
                }
                else -> { // !is WildcardSelector
                    val fieldSelectors = selectors.filterIsInstance<FieldSelector>().distinct()
                    if (fieldSelectors.isNotEmpty()) {
                        val fieldSelector = fieldSelectors.first()
                        if (fieldSelector.column != groupBy) {
                            throw IllFormSelectError("Field `${fieldSelector.column}` is not in GROUP BY")
                        } else if (fieldSelectors.size > 1) {
                            throw IllFormSelectError("Field `${fieldSelectors.last().column}` is not in the GROUP BY clause")
                        }
                    }
                    val groupKeyIndex = result.getHeaderIndex(groupBy.toString())
                    val groups = result.data.groupBy { row -> row[groupKeyIndex] }
                    val headers = selectors.map { selector ->
                        when (selector) {
                            is FieldSelector ->
                                selector to result.getHeaderIndex(selector.toString())
                            is AggregationSelector ->
                                selector to result.getHeaderIndex(selector.toString())
                            else /* WildCardCountSelector */ ->
                                selector to 0
                        }
                    }
                    return aggregate(headers, groups.values)
                }
            }
        } else { // groupBy == null
            when (selectors.first()) {
                is WildcardSelector -> {
                    return result
                }
                is FieldSelector -> {
                    val headers = selectors.map { selector ->
                        selector as FieldSelector
                        val header = selector.column.toString()
                        header to result.getHeaderIndex(header)
                    }
                    return SuccessResult(
                        headers.map { (header, _) -> header },
                        result.data.map { row -> headers.map { (_, index) -> row[index] } }
                    )
                }
                else -> { // aggregation
                    val headers = selectors.map { selector ->
                        when (selector) {
                            is AggregationSelector ->
                                selector to result.getHeaderIndex(selector.toString())
                            else /* WildCardCountSelector */ ->
                                selector to 0
                        }
                    }
                    return aggregate(headers, listOf(result.data))
                }
            }
        }
    }

    fun updateRecords(
        tableName: String,
        conditions: List<PredicateCondition>,
        map: Map<String, Any?>
    ): QueryResult {
        val (metaHandler, tableInfo) = this.selectTable(tableName)
        this.columnNameResolution(conditions.asSequence().map { it.column }, tableInfo)
        // TODO map 的合法性检查
        val map = map.map { (columnName, value) -> tableInfo.getColumnIndex(columnName) to value }
        val record = this.recordHandler.openRecord(metaHandler.dbName, tableName)
        val values = this.selectRecords(metaHandler, tableInfo, conditions).toList()
        values.forEach { (rid, oldRow) ->
            this.checkDeleteConstraints(metaHandler, tableInfo, oldRow)
            val newRow = oldRow.toMutableList()
            map.forEach { (columnIndex, value) ->
                newRow[columnIndex] = value
            }
            this.checkInsertConstraints(metaHandler, tableInfo, newRow)
            this.deleteIndex(metaHandler, tableInfo, rid, oldRow)
            record.updateRecord(rid, tableInfo.buildRecord(newRow))
            this.insertIndex(metaHandler, tableInfo, rid, newRow)
        }
        return SuccessResult(listOf("Updated"), listOf(listOf(values.size)))
    }

    fun deleteRecords(tableName: String, conditions: List<PredicateCondition>): QueryResult {
        val (metaHandler, tableInfo) = this.selectTable(tableName)
        this.columnNameResolution(conditions.asSequence().map { it.column }, tableInfo)
        val record = this.recordHandler.openRecord(metaHandler.dbName, tableName)
        val values = this.selectRecords(metaHandler, tableInfo, conditions).toList()
        values.forEach { (rid, row) ->
            this.checkDeleteConstraints(metaHandler, tableInfo, row)
            record.deleteRecord(rid)
            this.deleteIndex(metaHandler, tableInfo, rid, row)
        }
        return SuccessResult(listOf("Deleted"), listOf(listOf(values.size)))
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
            val row = tableInfo.parseRecord(record.getRecord(rid))
            val key = row[columnIndex] as Int
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

    private fun isDuplicated(
        metaHandler: MetaHandler,
        tableInfo: TableInfo,
        keys: List<Pair<Int, Any>>,
        self: RID? = null
    ): Boolean {
        val conditions = keys.map { (columnIndex, key) ->
            val column = UnqualifiedColumn(tableInfo.name, tableInfo.columns[columnIndex].name)
            val predicate = CompareWith(CompareOp.EQ_OP, key)
            PredicateCondition(column, predicate)
        }
        val result = this.selectRecords(metaHandler, tableInfo, conditions)
        return if (self == null) {
            result.iterator().hasNext()
        } else {
            result.any { (rid, _) -> rid == self }
        }
    }

    private fun checkInsertConstraints(
        metaHandler: MetaHandler,
        tableInfo: TableInfo,
        row: List<Any?>
    ) {
        // primary key
        if (tableInfo.primary.isNotEmpty()) {
            val keys = tableInfo.primary.map { columnName ->
                val columnIndex = tableInfo.getColumnIndex(columnName)
                columnIndex to row[columnIndex]!!
            }
            if (this.isDuplicated(metaHandler, tableInfo, keys)) {
                throw ConstraintViolationError(
                    "Duplicated primary key(s): ${
                        trimListToPrint(
                            keys.map { (_, value) -> value },
                            3
                        )
                    }"
                )
            }
        }
        // unique
        tableInfo.unique.forEach { columnName ->
            val columnIndex = tableInfo.getColumnIndex(columnName)
            val value = row[columnIndex]!!
            val keys = listOf(columnIndex to value)
            if (this.isDuplicated(metaHandler, tableInfo, keys)) {
                throw ConstraintViolationError("Duplicated unique key: `${value}`")
            }
        }
        // foreign key
        tableInfo.foreign.forEach { columnName, (foreignTableName, foreignColumnName) ->
            val columnIndex = tableInfo.getColumnIndex(columnName)
            val value = row[columnIndex]!!
            val foreignTableInfo = metaHandler.getTable(foreignTableName)
            val rootPageId = foreignTableInfo.indices[foreignColumnName]!!
            val index = this.indexManager.openIndex(
                metaHandler.dbName,
                foreignTableName,
                foreignColumnName,
                rootPageId
            )
            index.get(value as Int)
                ?: throw ConstraintViolationError("Missing foreign key `${columnName}`: `${value}`")
        }
    }

    private fun checkDeleteConstraints(
        metaHandler: MetaHandler,
        tableInfo: TableInfo,
        row: List<Any?>
    ) {
        // TODO foreign key
    }

    private fun insertIndex(
        metaHandler: MetaHandler,
        tableInfo: TableInfo,
        rid: RID,
        row: List<Any?>
    ) {
        tableInfo.indices.forEach { (columnName, rootPageId) ->
            val index = this.indexManager.openIndex(
                metaHandler.dbName,
                tableInfo.name,
                columnName,
                rootPageId
            )
            val columnIndex = tableInfo.getColumnIndex(columnName)
            index.put(row[columnIndex] as Int? ?: Int.MIN_VALUE, rid)
        }
    }

    private fun deleteIndex(
        metaHandler: MetaHandler,
        tableInfo: TableInfo,
        rid: RID,
        row: List<Any?>
    ) {
        tableInfo.indices.forEach { (columnName, rootPageId) ->
            val index = this.indexManager.openIndex(
                metaHandler.dbName,
                tableInfo.name,
                columnName,
                rootPageId
            )
            val columnIndex = tableInfo.getColumnIndex(columnName)
            index.remove(row[columnIndex] as Int? ?: Int.MIN_VALUE)
        }
    }

    private fun selectIndices(
        metaHandler: MetaHandler,
        tableInfo: TableInfo,
        conditions: List<PredicateCondition>
    ): Sequence<RID> {
        val ranges = mutableMapOf<String, Pair<Int, Int>>()
        conditions.asSequence()
            .filter { condition -> condition.tableName == tableInfo.name }
            .filter { condition -> tableInfo.existIndex(condition.columnName) }
            .forEach { (_, columnName, predicate) ->
                val columnInfo = tableInfo.columnMap[columnName]!!
                if (columnInfo.type == AttributeType.INT && predicate is CompareWith) {
                    val value = predicate.rhs as Int
                    when (predicate.op) {
                        CompareOp.EQ_OP -> ranges.compute(columnName) { _, boundary ->
                            if (boundary == null) {
                                value to value
                            } else {
                                val (l, r) = boundary
                                max(value, l) to min(value, r)
                            }
                        }
                        CompareOp.LT_OP -> ranges.compute(columnName) { _, boundary ->
                            if (boundary == null) {
                                Int.MIN_VALUE to value - 1
                            } else {
                                val (l, r) = boundary
                                l to min(value - 1, r)
                            }
                        }
                        CompareOp.GT_OP -> ranges.compute(columnName) { _, boundary ->
                            if (boundary == null) {
                                value + 1 to Int.MAX_VALUE
                            } else {
                                val (l, r) = boundary
                                max(value + 1, l) to r
                            }
                        }
                        CompareOp.LE_OP -> ranges.compute(columnName) { _, boundary ->
                            if (boundary == null) {
                                Int.MIN_VALUE to value
                            } else {
                                val (l, r) = boundary
                                l to min(value, r)
                            }
                        }
                        CompareOp.GE_OP -> ranges.compute(columnName) { _, boundary ->
                            if (boundary == null) {
                                value to Int.MAX_VALUE
                            } else {
                                val (l, r) = boundary
                                max(value, l) to r
                            }
                        }
                        CompareOp.NE_OP -> {}
                    }
                }
            }
        return ranges.map { (columnName, boundary) ->
            val index = this.indexManager.openIndex(
                metaHandler.dbName,
                tableInfo.name,
                "${tableInfo.name}.${columnName}",
                tableInfo.indices[columnName]!!
            )
            val (l, r) = boundary
            index.get(l, r).toSet()
        }.reduceOrNull(Set<RID>::intersect)?.asSequence() ?: run {
            val record = this.recordHandler.openRecord(metaHandler.dbName, tableInfo.name)
            record.getItemRIDs()
        }
    }

    private fun selectRecords(
        metaHandler: MetaHandler,
        tableInfo: TableInfo,
        conditions: List<PredicateCondition>
    ): Sequence<Pair<RID, List<Any?>>> {
        val record = this.recordHandler.openRecord(metaHandler.dbName, tableInfo.name)
        val test = selectIndices(metaHandler, tableInfo, conditions).toList()
        val values = this.selectIndices(metaHandler, tableInfo, conditions)
            .map { rid -> rid to tableInfo.parseRecord(record.getRecord(rid)) }
        val predicates = conditions.map { condition ->
            val columnIndex = tableInfo.getColumnIndex(condition.columnName)
            val columnInfo = tableInfo.columns[columnIndex]
            condition.predicate.build(columnIndex to columnInfo)
        }
        return values.filter { (_, row) -> predicates.all { p -> p(row) } }
    }

    private fun joinEquals(
        results: Map<String, SuccessResult>,
        joinConditions: List<JoinCondition>
    ): SuccessResult {
        val results = results.toMutableMap()
        // [((table_0, table_1), (column_0, column_1))]
        val columnPairs = joinConditions.map { (tableName, columnName,
                                                   targetTableName, targetColumnName) ->
            val column = Pair(tableName!!, columnName)
            val targetColumn = Pair(targetTableName!!, targetColumnName)
            listOf(column, targetColumn)
                .sortedWith { lhs, rhs -> lhs.compareTo(rhs) }
                .unzip()
                .map { (a, b) -> Pair(a, b) }
        }
        // (table_0, table_1) => [(column_0, column_1), ...]
        val joinPairs = mutableMapOf<Pair<String, String>, MutableList<Pair<String, String>>>()
        columnPairs.forEach { (tablePair, columnPair) ->
            joinPairs.getOrPut(tablePair) { mutableListOf() }.add(columnPair)
        }
        val tables = UnionFindSet(results.keys)
        val result = joinPairs.entries.fold(null as SuccessResult?) { _, (tablePair, columnPairs) ->
            val (outerTableName, innerTableName) = tablePair
            val outerResult = results[tables.find(outerTableName)]!!
            val innerResult = results[tables.find(innerTableName)]!!
            val outerColumns = columnPairs.map { (outerColumnName, _) ->
                "${outerTableName}.${outerColumnName}"
            }
            val innerColumns = columnPairs.map { (_, innerColumnName) ->
                "${innerTableName}.${innerColumnName}"
            }
            nestedLoopJoin(outerResult, innerResult, outerColumns, innerColumns).also { result ->
                val joinedTable = tables.union(outerTableName, innerTableName)
                results[joinedTable] = result
            }
        }
        return result!!
    }
}
