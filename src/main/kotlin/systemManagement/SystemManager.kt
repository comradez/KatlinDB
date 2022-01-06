package systemManagement

import metaManagement.info.ColumnInfo
import metaManagement.info.TableInfo
import parser.DatabaseVisitor
import parser.QueryResult
import java.io.File

class SystemManager(private val workDir: File) {
    private val visitor = DatabaseVisitor(this)

    val databases = run {
        assert(this.workDir.isDirectory)
        this.workDir
            .listFiles { dir -> dir.isDirectory }!!
            .mapTo(mutableSetOf<String>()) { it.name }
    }

    fun execute(sql: String): QueryResult {
        TODO()
    }

    fun createDatabase(databaseName: String): QueryResult {
        TODO()
    }

    fun dropDatabase(databaseName: String): QueryResult {
        TODO()
    }

    fun useDatabase(databaseName: String): QueryResult {
        TODO()
    }

    fun showTables(): List<String> {
        TODO()
    }

    fun createTable(info: TableInfo) {
        TODO()
    }

    fun describeTable(tableName: String): QueryResult {
        TODO()
    }

    fun renameTable(oldName: String, newName: String) {
        TODO()
    }

    fun dropTable(tableName: String) {
        TODO()
    }

    fun addColumn(tableName: String, info: ColumnInfo) {
        TODO()
    }

    fun dropColumn(tableName: String, columnName: String) {
        TODO()
    }

    fun <T> insertRecord(tableName: String, values: List<T>) {
        TODO()
    }

    // fun selectRecords(selectors: List<Selector>, tableNames: List<String>, conditions)

    fun addUnique(tableName: String, columnName: String) {
        TODO()
    }

    fun setPrimary(tableName: String, primary: List<String>) {
        TODO()
    }

    fun dropPrimary(tableName: String) {
        TODO()
    }

    fun addForeign(
        tableName: String,
        column: ColumnInfo,
        foreign: Pair<String, String>,
        foreignName: String? = null
    ) {
        TODO()
    }

    fun dropForeign(tableName: String, column: ColumnInfo, foreignName: String?) {
        TODO()
    }

    fun showIndices(): List<String> {
        TODO()
    }

    fun createIndex(tableName: String, columnName: String, indexName: String) {
        TODO()
    }

    fun renameIndex(oldName: String, newName: String) {
        TODO()
    }

    fun dropIndex(indexName: String) {
        TODO()
    }
}
