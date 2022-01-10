package metaManagement.info

import kotlinx.serialization.Serializable
import utils.InternalError

@Serializable
class DatabaseInfo(
    private val name: String,
    private val tables: List<TableInfo>
) {
    val tableMap = tables.associateBy { it.name }.toMutableMap()
    val indexMap = mutableMapOf<String, MutableSet<String>>() // tableName => [columnName, ...]

    fun insertTable(table: TableInfo) {
        if (table.name !in tableMap.keys) {
            tableMap[table.name] = table
        } else {
            throw InternalError("Table ${table.name} already exists.")
        }
    }

    fun insertColumn(tableName: String, column: ColumnInfo) {
        val table = tableMap[tableName] ?: throw InternalError("Table $tableName should exist.")
        table.insertColumn(column)
    }

    fun removeTable(tableName: String) {
        tableMap.remove(tableName) ?: throw InternalError("Table $tableName should exist.")
    }

    fun removeColumn(tableName: String, columnName: String): Int {
        val table = tableMap[tableName]
        return table?.removeColumn(columnName) ?: throw InternalError("Table $tableName should exist.")
    }

    fun createIndex(tableName: String, columnName: String) {
        this.indexMap.getOrPut(tableName) { mutableSetOf() }.add(columnName)
    }

    fun dropIndex(tableName: String, columnName: String) {
        this.indexMap[tableName]?.remove(columnName)
    }

    fun buildColumnToTableMap(tableNames: List<String>): Map<String, List<TableInfo>> {
        val columnToTable = hashMapOf<String, MutableList<TableInfo>>()
        for (tableName in tableNames) {
            val table = tableMap[tableName] ?: throw InternalError("Table $tableName should exist.")
            for (columnName in table.columnMap.keys) {
                if (columnName in columnToTable.keys) {
                    columnToTable[columnName]!!.add(table)
                } else {
                    columnToTable[columnName] = mutableListOf(table)
                }
            }
        }
        return columnToTable
    }
}
