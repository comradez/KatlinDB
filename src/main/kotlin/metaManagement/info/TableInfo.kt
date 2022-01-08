package metaManagement.info

import dataConverter.Converter
import kotlinx.serialization.Serializable
import recordManagement.Record
import utils.ColumnAlreadyExistsError
import utils.ColumnNotExistsError
import utils.InternalError

@Serializable
class TableInfo(
    val name: String,
    private val _columns: List<ColumnInfo>
) {
    val columns = _columns.toMutableList()
    var primary: List<String> = mutableListOf()
    val foreign = hashMapOf<String, Pair<String, String>>()
    val unique = hashSetOf<String>()
    val indices = hashMapOf<String, Int>()

    var columnMap = columns.associateBy { it.name }
    var sizeList = columns.map { it.getColumnSize() }
    var typeList = columns.map { it.type }
    var totalSize = sizeList.sum()
    private var columnIndices = columns.mapIndexed { i, column -> column.name to i }.toMap()

    private fun updateParams() {
        columnMap = columns.associateBy { it.name }
        sizeList = columns.map { it.getColumnSize() }
        typeList = columns.map { it.type }
        totalSize = sizeList.sum()
        columnIndices = columns.mapIndexed { i, column -> column.name to i }.toMap()
    }

    fun describe(): List<ColumnDescription> {
        val descriptionMap = columns.associate { it.name to it.getDescription() }
        for (columnName in primary) {
            descriptionMap[columnName]?.key = "PRI"
        }
        for (columnName in foreign.keys) {
            val key = descriptionMap[columnName]?.key
            descriptionMap[columnName]?.key = if (key != null) {
                "MUL"
            } else {
                "FOR"
            }
            requireNotNull(descriptionMap[columnName]?.key)
        }
        for (columnName in unique) {
            val key = descriptionMap[columnName]?.key
            descriptionMap[columnName]?.key = key ?: "UNI"
            requireNotNull(descriptionMap[columnName]?.key)
        }
        return descriptionMap.values.toList()
    }

    fun insertColumn(column: ColumnInfo) {
        if (column.name in columnMap.keys) {
            throw ColumnAlreadyExistsError(this.name, column.name)
        }
        columns.add(column)
        updateParams()
    }

    fun removeColumn(columnName: String): Int {
        if (columnName !in columnMap.keys) {
            throw ColumnNotExistsError(this.name, columnName)
        }
        return columns.indexOfFirst { it.name == columnName }.also { columns.removeAt(it) }
    }

    fun addForeign(columnName: String, foreignInfo: Pair<String, String>) {
        foreign[columnName] = foreignInfo
    }

    fun removeForeign(columnName: String) {
        foreign.remove(columnName)
    }

    fun addUnique(columnName: String) {
        unique.add(columnName)
    }

    fun buildRecord(valueList: List<Any?>): ByteArray {
        return Converter.encode(sizeList, typeList, totalSize, valueList)
    }

    fun parseRecord(record: Record): List<Any?> {
        return Converter.decode(sizeList, typeList, totalSize, record)
    }

    fun getColumnIndex(columnName: String) = requireNotNull(columnIndices[columnName])

    fun getHeader() = columnMap.keys.map { "${name}.$it" }

    fun existIndex(columnName: String) = columnName in indices.keys

    fun createIndex(columnName: String, rootId: Int) {
        if (columnName !in indices.keys) {
            indices[columnName] = rootId
        } else {
            throw InternalError("Column $columnName already has an index.")
        }
    }

    fun renameIndex(oldName: String, newName: String) {
        indices.remove(oldName)?.let { indices[newName] = it }
    }

    fun dropIndex(indexName: String) {
        indices.remove(indexName)
    }
}
