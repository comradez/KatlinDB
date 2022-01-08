package systemManagement

import metaManagement.info.ColumnInfo
import recordManagement.AttributeType
import recordManagement.CompareOp
import utils.TypeMismatchError

abstract class Predicate {
    abstract fun filter(
        rows: Sequence<List<Any?>>,
        column: Pair<Int, ColumnInfo>
    ): Sequence<List<Any?>>
}

class CompareWith(val op: CompareOp, val rhs: Any) : Predicate() {
    override fun filter(
        rows: Sequence<List<Any?>>,
        column: Pair<Int, ColumnInfo>
    ): Sequence<List<Any?>> {
        val (columnIndex, columnInfo) = column
        val compareToRhs: (List<Any?>) -> Int = when (columnInfo.type) {
            AttributeType.LONG -> when (rhs) {
                is Int -> { row -> (row[columnIndex] as Long?)?.compareTo(rhs) ?: -1 }
                is Long -> { row -> (row[columnIndex] as Long?)?.compareTo(rhs) ?: -1 }
                is Float -> { row -> (row[columnIndex] as Long?)?.compareTo(rhs) ?: -1 }
                is Double -> { row -> (row[columnIndex] as Long?)?.compareTo(rhs) ?: -1 }
                else -> throw TypeMismatchError(rhs)
            }
            AttributeType.INT -> when (rhs) {
                is Int -> { row -> (row[columnIndex] as Int?)?.compareTo(rhs) ?: -1 }
                is Long -> { row -> (row[columnIndex] as Int?)?.compareTo(rhs) ?: -1 }
                is Float -> { row -> (row[columnIndex] as Int?)?.compareTo(rhs) ?: -1 }
                is Double -> { row -> (row[columnIndex] as Int?)?.compareTo(rhs) ?: -1 }
                else -> throw TypeMismatchError(rhs)
            }
            AttributeType.FLOAT -> when (rhs) {
                is Int -> { row -> (row[columnIndex] as Float?)?.compareTo(rhs) ?: -1 }
                is Long -> { row -> (row[columnIndex] as Float?)?.compareTo(rhs) ?: -1 }
                is Float -> { row -> (row[columnIndex] as Float?)?.compareTo(rhs) ?: -1 }
                is Double -> { row -> (row[columnIndex] as Float?)?.compareTo(rhs) ?: -1 }
                else -> throw TypeMismatchError(rhs)
            }
            AttributeType.STRING -> when (rhs) {
                is String -> { row -> (row[columnIndex] as String?)?.compareTo(rhs) ?: -1 }
                else -> throw TypeMismatchError(rhs)
            }
        }
        val map = this.op.map()
        val predicate: (List<Any?>) -> Boolean = { row -> compareToRhs(row).let(map) }
        return rows.filter(predicate)
    }
}

class ExistsIn(_values: List<Any?>) : Predicate() {
    val values = _values.toSet()

    override fun filter(
        rows: Sequence<List<Any?>>,
        column: Pair<Int, ColumnInfo>
    ): Sequence<List<Any?>> {
        val (columnIndex, _) = column
        return rows.filter { row -> row[columnIndex] in this.values }
    }
}

class HasPattern(_pattern: String) : Predicate() {
    val pattern = run {
        var pattern = _pattern // 先把这些映到特殊字符保留下来
            .replace("%%", "\r")
            .replace("%?", "\n")
            .replace("%_", "\b")
        pattern = Regex.escape(pattern)
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

    override fun filter(
        rows: Sequence<List<Any?>>,
        column: Pair<Int, ColumnInfo>
    ): Sequence<List<Any?>> {
        val (columnIndex, columnInfo) = column
        if (columnInfo.type != AttributeType.STRING) {
            throw TypeMismatchError(columnInfo.name)
        }
        return rows.filter { row -> this.pattern.matches(row[columnIndex] as String) }
    }
}

class IsNull : Predicate() {
    override fun filter(
        rows: Sequence<List<Any?>>,
        column: Pair<Int, ColumnInfo>
    ): Sequence<List<Any?>> {
        val (columnIndex, _) = column
        return rows.filter { row -> row[columnIndex] == null }
    }
}

class IsNotNull : Predicate() {
    override fun filter(
        rows: Sequence<List<Any?>>,
        column: Pair<Int, ColumnInfo>
    ): Sequence<List<Any?>> {
        val (columnIndex, _) = column
        return rows.filter { row -> row[columnIndex] != null }
    }
}

data class Condition(val tableName: String?, val columnName: String, val predicate: Predicate) {
}
