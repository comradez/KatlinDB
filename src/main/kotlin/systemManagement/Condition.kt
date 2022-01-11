package systemManagement

import metaManagement.info.ColumnInfo
import recordManagement.AttributeType
import recordManagement.CompareOp
import utils.ParseDate.Static.parseDate
import utils.TypeMismatchError

interface Predicate {
    fun build(column: Pair<Int, ColumnInfo>): (List<Any?>) -> Boolean
}

class CompareWith(val op: CompareOp, val rhs: Any) : Predicate {
    override fun build(column: Pair<Int, ColumnInfo>): (List<Any?>) -> Boolean {
        val (columnIndex, columnInfo) = column
        val compareToRhs: (List<Any?>) -> Int? = when (columnInfo.type) {
            AttributeType.LONG -> when (rhs) {
                is Int -> { row -> (row[columnIndex] as Long?)?.compareTo(rhs) }
                is Long -> { row -> (row[columnIndex] as Long?)?.compareTo(rhs) }
                is Float -> { row -> (row[columnIndex] as Long?)?.compareTo(rhs) }
                is Double -> { row -> (row[columnIndex] as Long?)?.compareTo(rhs) }
                is String -> { row -> (row[columnIndex] as Long?)?.compareTo(parseDate(rhs)) }
                else -> throw TypeMismatchError(rhs)
            }
            AttributeType.INT -> when (rhs) {
                is Int -> { row -> (row[columnIndex] as Int?)?.compareTo(rhs) }
                is Long -> { row -> (row[columnIndex] as Int?)?.compareTo(rhs) }
                is Float -> { row -> (row[columnIndex] as Int?)?.compareTo(rhs) }
                is Double -> { row -> (row[columnIndex] as Int?)?.compareTo(rhs) }
                else -> throw TypeMismatchError(rhs)
            }
            AttributeType.FLOAT -> when (rhs) {
                is Int -> { row -> (row[columnIndex] as Float?)?.compareTo(rhs) }
                is Long -> { row -> (row[columnIndex] as Float?)?.compareTo(rhs) }
                is Float -> { row -> (row[columnIndex] as Float?)?.compareTo(rhs) }
                is Double -> { row -> (row[columnIndex] as Float?)?.compareTo(rhs) }
                else -> throw TypeMismatchError(rhs)
            }
            AttributeType.STRING -> when (rhs) {
                is String -> { row ->
                    (row[columnIndex] as String?)?.run {
                        if (this.isEmpty()) null
                        else this.compareTo(rhs)
                    }
                }
                else -> throw TypeMismatchError(rhs)
            }
        }
        val map = this.op.map()
        return { row -> compareToRhs(row).let(map) }
    }
}

class ExistsIn(_values: List<Any?>) : Predicate {
    val values = _values.toSet()

    override fun build(column: Pair<Int, ColumnInfo>): (List<Any?>) -> Boolean {
        val (columnIndex, _) = column
        return { row -> row[columnIndex] in this.values }
    }
}

class HasPattern(_pattern: String) : Predicate {
    val pattern = run {
        var pattern = _pattern // 先把这些映到特殊字符保留下来
            .replace("%%", "\r")
            .replace("%?", "\n")
            .replace("%_", "\b")
        pattern = Regex.escape(pattern)
        pattern = pattern.substring(2 until pattern.length - 2)
        pattern = pattern
            .replace("%", ".*")
            .replace("\\?", ".")
            .replace("_", ".")
        pattern = pattern // 再变回去
            .replace("\r", "%")
            .replace("\n", "\\?")
            .replace("\b", "\\?")
        Regex("^${pattern}$")
    }

    override fun build(column: Pair<Int, ColumnInfo>): (List<Any?>) -> Boolean {
        val (columnIndex, columnInfo) = column
        if (columnInfo.type != AttributeType.STRING) {
            throw TypeMismatchError(columnInfo.name)
        }
        return { row -> this.pattern.matches(row[columnIndex] as String) }
    }
}

class IsNull : Predicate {
    override fun build(column: Pair<Int, ColumnInfo>): (List<Any?>) -> Boolean {
        val (columnIndex, _) = column
        return { row -> row[columnIndex] == null }
    }
}

class IsNotNull : Predicate {
    override fun build(column: Pair<Int, ColumnInfo>): (List<Any?>) -> Boolean {
        val (columnIndex, _) = column
        return { row -> row[columnIndex] != null }
    }
}

abstract class Condition(val column: UnqualifiedColumn) {
    val tableName get() = this.column.tableName
    val columnName get() = this.column.columnName

    operator fun component1() = this.tableName
    operator fun component2() = this.columnName
}

class PredicateCondition(
    unqualifiedColumn: UnqualifiedColumn,
    _predicate: Predicate
) : Condition(unqualifiedColumn) {
    val predicate = _predicate

    operator fun component3() = this.predicate
}

class JoinCondition(
    column: UnqualifiedColumn,
    val targetColumn: UnqualifiedColumn
) : Condition(column) {
    val targetTableName get() = this.targetColumn.tableName
    val targetColumnName get() = this.targetColumn.columnName

    operator fun component3() = this.targetTableName
    operator fun component4() = this.targetColumnName
}
