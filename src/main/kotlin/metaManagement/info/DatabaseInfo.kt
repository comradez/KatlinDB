package metaManagement.info

import kotlinx.serialization.Serializable
import utils.InternalError

@Serializable
class DatabaseInfo(
    private val name: String,
    private val tables: List<TableInfo>
) {
    val tableMap = tables.associateBy { it.name }.toMutableMap()
    val indexMap = hashMapOf<String, Pair<String, String>>()

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

    fun createIndex(indexName: String, tableName: String, columnName: String) {
        if (indexName !in indexMap.keys) {
            indexMap[indexName] = tableName to columnName
        } else {
            throw InternalError("Index $indexName already exists.")
        }
    }

    fun dropIndex(indexName: String) {
        val index = indexMap[indexName]
        index ?: throw InternalError("Index $indexName should exist.")
    }

    fun getIndexInfo(indexName: String): Pair<String, String> {
        val index = indexMap[indexName]
        return index ?: throw InternalError("Index $indexName should exist.")
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
