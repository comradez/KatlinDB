package metaManagement

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import metaManagement.info.ColumnInfo
import metaManagement.info.DatabaseInfo
import metaManagement.info.TableInfo
import utils.InternalError
import java.io.File

import kotlinx.serialization.json.Json

class MetaHandler(
    _dbName: String,
    _homeDirectory: String
) {
    val dbName = _dbName
    private val homeDirectory = _homeDirectory
    private val metaDirectory = "$homeDirectory/$dbName/$dbName.meta"
    lateinit var dbInfo: DatabaseInfo
    init {
        val metaFile = File(metaDirectory)
        if (metaFile.exists()) {
            load()
        } else {
            dbInfo = DatabaseInfo(dbName, arrayListOf())
            dump()
        }
    }

    fun addTable(table: TableInfo) {
        dbInfo.insertTable(table)
        dump()
    }

    fun addColumn(tableName: String, column: ColumnInfo) {
        dbInfo.insertColumn(tableName, column)
        dump()
    }

    fun removeTable(tableName: String) {
        dbInfo.removeTable(tableName)
        dump()
    }

    fun removeColumn(tableName: String, columnName: String): Int {
        return dbInfo.removeColumn(tableName, columnName).also { dump() }
    }

    fun getColumn(tableName: String, columnName: String): ColumnInfo {
        val table = dbInfo.tableMap[tableName] ?: throw InternalError("No table $tableName in database $dbName found.")
        return table.columnMap[columnName] ?: throw InternalError("No column $columnName in table ${table.name} found.")
    }

    fun getColumnIndex(tableName: String, columnName: String): Int {
        val table = dbInfo.tableMap[tableName] ?: throw InternalError("No table $tableName in database $dbName found.")
        return table.getColumnIndex(columnName)
    }

    fun getTable(tableName: String): TableInfo {
        return dbInfo.tableMap[tableName] ?: throw InternalError("No table $tableName in database $dbName found.")
    }

    fun existIndex(tableName: String, columnName: String) =
        this.dbInfo.indexMap[tableName]?.contains(columnName) ?: false

    fun createIndex(tableName: String, columnName: String) {
        dbInfo.createIndex(tableName, columnName)
        dump()
    }

    fun dropIndex(tableName: String, columnName: String) {
        dbInfo.dropIndex(tableName, columnName)
        dump()
    }

    fun setPrimary(tableName: String, primary: List<String>) {
        val table = dbInfo.tableMap[tableName] ?: throw InternalError("No table $tableName in database $dbName found.")
        table.primary = primary.toMutableList()
    }

    fun dropPrimary(tableName: String) {
        val table = dbInfo.tableMap[tableName] ?: throw InternalError("No table $tableName in database $dbName found.")
        table.primary = mutableListOf()
    }

    fun addForeign(tableName: String, columnName: String, foreign: Pair<String, String>) {
        val table = dbInfo.tableMap[tableName] ?: throw InternalError("No table $tableName in database $dbName found.")
        table.addForeign(columnName, foreign)
        dump()
    }

    fun removeForeign(tableName: String, columnName: String) {
        val table = dbInfo.tableMap[tableName] ?: throw InternalError("No table $tableName in database $dbName found.")
        table.removeForeign(columnName)
        dump()
    }

    fun addUnique(tableName: String, columnName: String) {
        val table = dbInfo.tableMap[tableName] ?: throw InternalError("No table $tableName in database $dbName found.")
        table.addUnique(columnName)
    }

    fun renameTable(tableName: String, newTableName: String) {
        val table = dbInfo.tableMap.remove(tableName) ?: throw InternalError("No table $tableName in database $dbName found.")
        dbInfo.tableMap[newTableName] = table
        dbInfo.indexMap.remove(tableName)?.let { dbInfo.indexMap[newTableName] = it }
        dump()
    }

    fun close() {
        dump()
    }

    private fun load() {
        val destFile = File(metaDirectory)
        dbInfo = Json.decodeFromString(destFile.readText())
    }

    private fun dump() {
        val destFile = File(metaDirectory)
        destFile.writeBytes(Json.encodeToString(dbInfo).toByteArray())
    }
}
