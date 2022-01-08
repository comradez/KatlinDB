package parser

import metaManagement.info.ColumnInfo
import metaManagement.info.TableInfo
import recordManagement.AttributeType
import recordManagement.CompareOp
import recordManagement.buildAttributeType
import recordManagement.buildCompareOp
import systemManagement.*
import utils.InternalError
import java.lang.Exception

@Suppress("NAME_SHADOWING")
class DatabaseVisitor(private val manager: SystemManager) : SQLBaseVisitor<Any>() {

    private var lastStartTime: Long? = null

    private fun measureTimeCost(): Long? {
        val currentTime = System.nanoTime()
        return if (lastStartTime != null) {
            val cost = currentTime - lastStartTime!!
            lastStartTime = currentTime
            cost
        } else {
            lastStartTime = currentTime
            null
        }
    }

    override fun visitProgram(ctx: SQLParser.ProgramContext?): List<QueryResult> {
        val results = mutableListOf<QueryResult>()
        for (statement in ctx!!.statement()) {
            try {
                when (val result = statement.accept(this)) {
                    is QueryResult -> {
                        result.timeCost = measureTimeCost()
                        results.add(result)
                    }
                    is Unit -> {
                        val emptyResult = EmptyResult()
                        emptyResult.timeCost = measureTimeCost()
                        results.add(emptyResult)
                    }
                    else -> throw InternalError("Bad result type")
                }
            } catch (e: Exception) {
                val errorResult = ErrorResult(e.message ?: "Unknown Error")
                results.add(errorResult)
                break
            }
        }
        return results
    }

    override fun visitStatement(ctx: SQLParser.StatementContext?): Any {
        return if (ctx!!.db_statement() != null) {
            ctx.db_statement().accept(this) // Unit 或者 QueryResult
        } else if (ctx.io_statement() != null) {
            ctx.io_statement().accept(this) // 还没实现
        } else if (ctx.table_statement() != null) {
            ctx.table_statement().accept(this) // Unit 或者 QueryResult
        } else if (ctx.alter_statement() != null) {
            ctx.alter_statement().accept(this) // 还没实现
        } else if (ctx.Annotation() != null || ctx.Null() != null) {
            Unit // 确定就是 Unit
        } else {
            throw InternalError("Bad Statement")
        }
    }

    override fun visitCreate_db(ctx: SQLParser.Create_dbContext?) {
        manager.createDatabase(ctx!!.Identifier().toString())
    }

    override fun visitDrop_db(ctx: SQLParser.Drop_dbContext?) {
        manager.dropDatabase(ctx!!.Identifier().toString())
    }

    override fun visitShow_dbs(ctx: SQLParser.Show_dbsContext?): QueryResult {
        return SuccessResult(listOf("databases"), listOf(manager.showDatabases().toList()))
    }

    override fun visitUse_db(ctx: SQLParser.Use_dbContext?) {
        manager.useDatabase(ctx!!.Identifier().toString())
    }

    override fun visitShow_tables(ctx: SQLParser.Show_tablesContext?): QueryResult {
        return SuccessResult(listOf("tables"), listOf(manager.showTables()))
    }

    override fun visitShow_indexes(ctx: SQLParser.Show_indexesContext?): QueryResult {
        return SuccessResult(listOf("indices"), listOf(manager.showIndices()))
    }

    override fun visitLoad_data(ctx: SQLParser.Load_dataContext?) {
        val filename = ctx!!.String().toString()
        val tableName = ctx.Identifier().toString()
        manager.load(filename, tableName)
    }

    override fun visitDump_data(ctx: SQLParser.Dump_dataContext?) {
        val filename = ctx!!.String().toString()
        val tableName = ctx.Identifier().toString()
        manager.dump(filename, tableName)
    }

    override fun visitCreate_table(ctx: SQLParser.Create_tableContext?) {
        val (_columns, _foreignKeys, _primaryKey) = ctx!!.field_list().accept(this)!! as Triple<*, *, *>
        val columns = (_columns as List<*>).map { it as ColumnInfo }
        val foreignKeys = _foreignKeys as HashMap<*, *> // 实际上是 HashMap<String, Pair<String, String>>
        val primaryKey = (_primaryKey as List<*>?)?.map { it as String }.orEmpty()
        val tableName = ctx.Identifier().toString()
        manager.createTable(TableInfo(tableName, columns))
        for ((_columnInfo, _columnMessage) in foreignKeys) {
            val columnInfo = _columnInfo as ColumnInfo
            val columnMessage = (_columnMessage as Pair<*, *>).first as String to _columnMessage.second as String
            manager.addForeign(tableName, columnInfo.name, columnMessage)
        }
        manager.setPrimary(tableName, primaryKey)
    }

    override fun visitDrop_table(ctx: SQLParser.Drop_tableContext?) {
        val tableName = ctx!!.Identifier().toString()
        manager.dropTable(tableName)
    }

    override fun visitDescribe_table(ctx: SQLParser.Describe_tableContext?): QueryResult {
        val tableName = ctx!!.Identifier().toString()
        return manager.describeTable(tableName)
    }

    override fun visitInsert_into_table(ctx: SQLParser.Insert_into_tableContext?): QueryResult {
        val tableName = ctx!!.getChild(2).toString()
        val valueLists = ctx.value_lists().accept(this) as List<*>
        // 实际上是 List<List<Any?>>
        for (_valueList in valueLists) {
            val valueList = _valueList as List<Any?>
            manager.insertRecord(tableName, valueList)
        }
        return SuccessResult(listOf("insertedItemNumber"), listOf(listOf(valueLists.size.toString())))
    }

    override fun visitDelete_from_table(ctx: SQLParser.Delete_from_tableContext?): QueryResult {
        val tableName = ctx!!.Identifier().toString()
        val conditions = (ctx.where_and_clause().accept(this) as List<*>).map { it as Condition }
        return manager.deleteRecords(tableName, conditions)
    }

    override fun visitUpdate_table(ctx: SQLParser.Update_tableContext?): QueryResult {
        val tableName = ctx!!.Identifier().toString()
        val conditions = (ctx.where_and_clause().accept(this) as List<*>)
            .map { it as Condition }
        val setValueMap = (ctx.set_clause().accept(this) as HashMap<*, *>)
            .map { (key, value) -> key as String to value as Any? }.toMap()
        return manager.updateRecords(
            tableName,
            conditions,
            setValueMap
        )
    }

    override fun visitSelect_table_(ctx: SQLParser.Select_table_Context?): SuccessResult {
        return ctx!!.select_table().accept(this) as SuccessResult
    }

    override fun visitSelect_table(ctx: SQLParser.Select_tableContext?): QueryResult {
        val tableNames = (ctx!!.identifiers().accept(this) as List<*>).map { it as String }
        val conditions = (ctx.where_and_clause().accept(this) as List<*>).map { it as Condition }
        val selectors = (ctx.selectors().accept(this) as List<*>).map { it as Selector }
        val groupBy = if (ctx.column() != null) {
            val groupByTemp = ctx.column().accept(this)
            (groupByTemp as Pair<*, *>).first as String to groupByTemp.second as String
        } else { null }
        val (limit, offset) = if (ctx.Integer() != null) {
            ctx.Integer(0).toString().toInt() to ctx.Integer(1).toString().toInt()
        } else {
            -1 to -1
        }
        return manager.selectRecords(
            selectors,
            tableNames,
            conditions,
            groupBy,
            limit,
            offset
        )
    }

    override fun visitAlter_add_index(ctx: SQLParser.Alter_add_indexContext?) {
        val tableName = ctx!!.Identifier().toString()
        val columnNames = (ctx.identifiers().accept(this) as List<*>).map { it as String }
        for (columnName in columnNames) {
            val indexName = "$tableName.$columnName"
            manager.createIndex(tableName, columnName, indexName)
        }
    }

    override fun visitAlter_drop_index(ctx: SQLParser.Alter_drop_indexContext?) {
        val tableName = ctx!!.Identifier().toString()
        val columnNames = (ctx.identifiers().accept(this) as List<*>).map { it as String }
        for (columnName in columnNames) {
            val indexName = "$tableName.$columnName"
            manager.dropIndex(indexName)
        }
    }

    override fun visitAlter_table_drop_pk(ctx: SQLParser.Alter_table_drop_pkContext?) {
        val tableName = ctx!!.Identifier(0).toString()
        val primaryKeyColumnName = ctx.Identifier(1)?.toString()
        // 明明一张表只有一列是主键为什么还要显式把键名写出来
        manager.dropPrimary(tableName)
    }

    override fun visitAlter_table_drop_foreign_key(ctx: SQLParser.Alter_table_drop_foreign_keyContext?) {
        val tableName = ctx!!.Identifier(0).toString()
        val columnName = ctx.Identifier(1).toString()
        manager.dropForeign(tableName, columnName)
    }

    override fun visitAlter_table_add_pk(ctx: SQLParser.Alter_table_add_pkContext?) {
        val tableName = ctx!!.Identifier(0).toString()
        val primary = (ctx.identifiers().accept(this) as List<*>).map { it as String }
        manager.setPrimary(tableName, primary)
    }

    override fun visitAlter_table_add_foreign_key(ctx: SQLParser.Alter_table_add_foreign_keyContext?) {
        val tableName = ctx!!.Identifier(0).toString()
        val foreignTableName = ctx.Identifier(2).toString()
        val columnNames = (ctx.identifiers(0).accept(this) as List<*>).map { it as String }
        val referNames = (ctx.identifiers(1).accept(this) as List<*>).map { it as String }
        for ((columnName, referName) in columnNames zip referNames) {
            manager.addForeign(tableName, columnName, foreignTableName to referName)
        }
    }

    override fun visitAlter_table_add_unique(ctx: SQLParser.Alter_table_add_uniqueContext?) {
        val tableName = ctx!!.Identifier().toString()
        val columnNames = (ctx.identifiers().accept(this) as List<*>).map { it as String }
        for (columnName in columnNames) {
            manager.addUnique(tableName, columnName)
        }
    }

    override fun visitField_list(ctx: SQLParser.Field_listContext?)
    : Triple<List<ColumnInfo>, HashMap<String, Pair<String, String>>, List<String>?> {
        val nameToColumn = hashMapOf<String, ColumnInfo>()
        val foreignKeys = hashMapOf<String, Pair<String, String>>()
        var primaryKey: List<String>? = null
        for (field in ctx!!.field()) {
            when (field!!) {
                is SQLParser.Normal_fieldContext -> {
                    val field = field as SQLParser.Normal_fieldContext
                    // 这里将变量遮蔽的使用限制在确定具体类型后的分支内部，不会引起混乱，因此 suppress 掉了 warning
                    val name = field.Identifier().toString()
                    val (_type, _size) = field.type_().accept(this)!! as Pair<*, *>
                    // 实际上是 Pair<AttributeType, Int>
                    val type = _type as AttributeType
                    val size = _size as Int
                    nameToColumn[name] = ColumnInfo(type, name, size)
                }
                is SQLParser.Primary_key_fieldContext -> {
                    val nameList = mutableListOf<String>()
                    val field = field as SQLParser.Primary_key_fieldContext
                    val names = field.accept(this) as List<*>
                    // 实际上是 List<String>
                    for (name in names) {
                        val name = name as String
                        if (name !in nameToColumn.keys) {
                            throw InternalError("field $name not found.")
                        }
                    }
                    if (primaryKey != null) {
                        throw InternalError("Only one primary key is supported.")
                    }
                    primaryKey = names.map { it as String }
                }
                is SQLParser.Foreign_key_fieldContext -> {
                    val field = field as SQLParser.Foreign_key_fieldContext
                    val (_fieldNames, _tableName, _referNames) = field.accept(this)!! as Triple<*, *, *>
                    val fieldNames = (_fieldNames as List<*>).map { it as String }
                    val tableName = _tableName as String
                    val referNames = (_referNames as List<*>).map { it as String }
                    for ((fieldName, referName) in fieldNames zip referNames) {
                        if (fieldName in foreignKeys.keys) {
                            throw InternalError("Foreign key $fieldName is duplicated with existing ones.")
                        }
                        foreignKeys[fieldName] = tableName to referName
                    }
                }
                else -> throw InternalError("An abstract field node is not allowed")
            }
        }
        return Triple(nameToColumn.values.toList(), foreignKeys, primaryKey)
    }

    override fun visitNormal_field(ctx: SQLParser.Normal_fieldContext?): ColumnInfo {
        val name = ctx!!.Identifier().toString()
        val (_type, _size) = ctx.type_().accept(this)!! as Pair<*, *>
        val type = _type as AttributeType
        val size = _size as Int
        // 虽然下边 visitType_ 返回的是 Pair<AttributeType, Int> 但是 accept 出来的东西还是 Any
        // 只能用最丑陋的方法强行 cast
        return ColumnInfo(type, name, size)
    }

    override fun visitPrimary_key_field(ctx: SQLParser.Primary_key_fieldContext?): List<*> {
       return ctx!!.identifiers().accept(this)!! as List<*>
        // 其实是 List<String>
    }

    override fun visitForeign_key_field(ctx: SQLParser.Foreign_key_fieldContext?)
    : Triple<List<String>, String, List<String>> {
        val tableName = if (ctx!!.Identifier().size > 1) {
            ctx.Identifier(1).toString()
        } else { ctx.Identifier(0).toString() }
        val fieldNames = (ctx.identifiers(0).accept(this) as List<*>).map { it as String }
        val referNames = (ctx.identifiers(1).accept(this) as List<*>).map { it as String }
        return Triple(
            fieldNames,
            tableName,
            referNames
        )
    }

    override fun visitType_(ctx: SQLParser.Type_Context?): Pair<AttributeType, Int> {
        val type = buildAttributeType(ctx!!.getChild(0).toString())
        val size = ctx.Integer().toString().toInt() ?: 0
        return type to size
    }

    override fun visitValue_lists(ctx: SQLParser.Value_listsContext?): List<List<Any?>> {
        return ctx!!.value_list().map { it.accept(this) as List<Any?> }
    }

    override fun visitValue_list(ctx: SQLParser.Value_listContext?): List<Any?> {
        return ctx!!.value().map { it.accept(this) }
    }

    override fun visitValue(ctx: SQLParser.ValueContext?): Any? {
        return if (ctx!!.Integer() != null) {
            ctx.Integer().toString().toInt()
        } else if (ctx.Float() != null) {
            ctx.Float().toString().toFloat()
        } else if (ctx.String() != null) {
            ctx.String().toString()
        } else if (ctx.Null()) {
            null
        } else {
            throw InternalError("Bad value type.")
        }
    }

    override fun visitWhere_and_clause(ctx: SQLParser.Where_and_clauseContext?): List<Condition> {
        return ctx!!.where_clause().map { it.accept(this) as Condition }
    }

    override fun visitWhere_operator_expression(ctx: SQLParser.Where_operator_expressionContext?): Condition {
        val (_tableName, _columnName) = ctx!!.column().accept(this) as Pair<*, *>
        val tableName = _tableName as String?
        val columnName = _columnName as String
        val operator = buildCompareOp(ctx.operator_().toString())
        val value = ctx.expression().accept(this)!!
        return Condition(tableName, columnName, CompareWith(operator, value))
    }

    override fun visitWhere_operator_select(ctx: SQLParser.Where_operator_selectContext?): Condition {
        val (_tableName, _columnName) = ctx!!.column().accept(this) as Pair<*, *>
        val tableName = _tableName as String?
        val columnName = _columnName as String
        val operator = buildCompareOp(ctx.operator_().toString())
        val result = ctx.select_table().accept(this) as SuccessResult
        val value = manager.resultToValue(result, false)
        return Condition(tableName, columnName, CompareWith(operator, value))
    }

    override fun visitWhere_null(ctx: SQLParser.Where_nullContext?): Condition {
        val (_tableName, _columnName) = ctx!!.column().accept(this) as Pair<*, *>
        val tableName = _tableName as String?
        val columnName = _columnName as String
        val predicate = if (ctx.getChild(2).toString().uppercase() == "NOT") {
            IsNotNull() // xx IS NOT NULL
        } else {
            IsNull() // xx IS NULL
        }
        return Condition(tableName, columnName, predicate)
    }

    override fun visitWhere_in_list(ctx: SQLParser.Where_in_listContext?): Condition {
        val (_tableName, _columnName) = ctx!!.column().accept(this) as Pair<*, *>
        val tableName = _tableName as String?
        val columnName = _columnName as String
        val values = ctx.value_list().accept(this) as List<Any?>
        return Condition(tableName, columnName, ExistsIn(values))
    }

    override fun visitWhere_in_select(ctx: SQLParser.Where_in_selectContext?): Condition {
        val (_tableName, _columnName) = ctx!!.column().accept(this) as Pair<*, *>
        val tableName = _tableName as String?
        val columnName = _columnName as String
        val result = ctx.select_table().accept(this) as SuccessResult
        val value = manager.resultToValue(result, true) as List<Any?>
        // 这个 value 是 select 出的结果，必须得是 List 才可以，不然不能 existIn
        return Condition(tableName, columnName, ExistsIn(value))
    }

    override fun visitWhere_like_string(ctx: SQLParser.Where_like_stringContext?): Condition {
        val (_tableName, _columnName) = ctx!!.column().accept(this) as Pair<*, *>
        val tableName = _tableName as String?
        val columnName = _columnName as String
        val pattern = ctx.String().toString().substring(
            1 until ctx.String().toString().length - 1
        )
        return Condition(tableName, columnName, HasPattern(pattern))
    }

    override fun visitColumn(ctx: SQLParser.ColumnContext?): Pair<String?, String> {
        return ctx!!.Identifier(0).toString() to ctx.Identifier(1).toString()
    }

    override fun visitExpression(ctx: SQLParser.ExpressionContext?): Any? {
        return if (ctx!!.value() != null) {
            ctx.value().accept(this)
        } else if (ctx.column() != null) {
            ctx.column().accept(this) // 注意这个东西的返回类型是 visitColumn 的，也就是 Pair<String, String>
        } else {
            throw InternalError("Bad expression: neither value nor column")
        }
    }

    override fun visitSet_clause(ctx: SQLParser.Set_clauseContext?): HashMap<String, Any?> {
        val setValueMap = hashMapOf<String, Any?>()
        for ((identifier, value) in ctx!!.Identifier() zip ctx.value()) {
            setValueMap[identifier.toString()] = value.accept(this)
        }
        return setValueMap
    }

    override fun visitSelectors(ctx: SQLParser.SelectorsContext?): List<Selector> {
        return if (ctx!!.getChild(0).toString() == Selector.WILDCARD) {
            listOf(FieldSelector(Selector.WILDCARD, Selector.WILDCARD))
        } else {
            ctx.selector().map { it.accept(this) as Selector }
        }
    }

    override fun visitSelector(ctx: SQLParser.SelectorContext?): Selector {
        if (ctx!!.Count() != null) {
            return CountSelector()
        }
        val (_tableName, _columnName) = ctx.column().accept(this) as Pair<*, *>
        val tableName = _tableName as String?
        val columnName = _columnName as String
        return if (ctx.aggregator() != null) {
            AggregationSelector(tableName, columnName, buildAggregator(ctx.aggregator().toString()))
        } else {
            FieldSelector(tableName, columnName)
        }
    }

    override fun visitIdentifiers(ctx: SQLParser.IdentifiersContext?): List<String> {
        return ctx!!.Identifier().map { it.toString() }
    }

    override fun visitOperator_(ctx: SQLParser.Operator_Context?): CompareOp {
        return buildCompareOp(ctx!!.toString())
    }

    override fun visitAggregator(ctx: SQLParser.AggregatorContext?): AggregationSelector.Aggregator {
        return buildAggregator(ctx!!.toString())
    }
}
