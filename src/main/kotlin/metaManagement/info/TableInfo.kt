package metaManagement.info

import dataConverter.Converter
import kotlinx.serialization.Serializable
import recordManagement.Record
import utils.ColumnExistError
import utils.InternalError

@Serializable
class TableInfo(
    val name: String,
    private val _columns: List<ColumnInfo>
) {
    private val columns = _columns.toMutableList()
    var primary: List<String> = mutableListOf()
    val foreign = hashMapOf<String, Pair<String, String>>()
    val unique = hashMapOf<String, String>()
    val indices = hashMapOf<String, Int>()

    var columnMap = columns.associateBy { it.name }
    private var sizeList = columns.map { it.getColumnSize() }
    private var typeList = columns.map { it.type }
    private var totalSize = sizeList.sum()
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
        for (columnName in unique.keys) {
            val key = descriptionMap[columnName]?.key
            descriptionMap[columnName]?.key = key ?: "UNI"
            requireNotNull(descriptionMap[columnName]?.key)
        }
        return descriptionMap.values.toList()
    }

    fun insertColumn(column: ColumnInfo) {
        if (column.name in columnMap.keys) {
            throw ColumnExistError("Column ${column.name} already exists.")
        }
        columns.add(column)
        updateParams()
    }

    fun removeColumn(columnName: String) {
        if (columnName !in columnMap.keys) {
            throw ColumnExistError("Column $columnName should exist.")
        }
        columns.removeAt(columns.indexOfFirst { it.name == columnName })
    }

    fun addForeign(column: ColumnInfo, foreignInfo: Pair<String, String>) {
        foreign[column.name] = foreignInfo
    }

    fun removeForeign(column: ColumnInfo) {
        foreign.remove(column.name)
    }

    fun addUnique(column: ColumnInfo, uniqueInfo: String) {
        unique[column.name] = uniqueInfo
    }

    fun buildRecord(valueList: List<Any>): ByteArray {
        return Converter.encode(sizeList, typeList, totalSize, valueList)
    }

    fun parseRecord(record: Record): List<Any> {
        return Converter.decode(sizeList, typeList, totalSize, record)
    }

    fun getColumnIndex(columnName: String) = requireNotNull(columnIndices[columnName])

    fun getHeader() = columnMap.keys.map { "${name}.$it" }

    fun existIndex(column: ColumnInfo) = column.name in indices.keys

    fun createIndex(column: ColumnInfo, rootId: Int) {
        if (column.name !in indices.keys) {
            indices[column.name] = rootId
        } else {
            throw InternalError("Column ${column.name} already has an index.")
        }
    }

    fun dropIndex(column: ColumnInfo) {
        indices.remove(column.name)
    }
}