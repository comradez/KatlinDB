package parser

import org.sk.PrettyTable
import utils.IllFormSelectError
import utils.trimListToPrint
import java.sql.Date

abstract class QueryResult {
    var timeCost: Long? = null
}

class EmptyResult : QueryResult()

class ErrorResult(val errorMessage: String) : QueryResult()

class SuccessResult(val headers: List<String>, val data: List<List<Any?>>, val extra: List<String>? = null) : QueryResult() {
    init {
        assert(this.data.all { row -> row.size == this.headers.size })
    }

    val columnIndices get() = this.headers.indices
    val columnSize get() = this.headers.size
    val rowSize get() = this.data.size
    val aliases get() = this.aliasMap.entries

    private val headerIndices = this.headers.withIndex().associate { (index, header) ->
        header to index
    }
    private val aliasMap = mutableMapOf<String, String>() // alias => header

    fun getHeaderIndex(header: String): Int =
        this.headerIndices[this.aliasMap.getOrDefault(header, header)]!!

    fun setAlias(alias: String, header: String) {
        this.aliasMap[alias] = header
    }

    fun setAliases(map: Sequence<Pair<String, String>>) {
        this.aliasMap.putAll(map)
    }

    fun toColumnForOuterSelect(): Sequence<Any?> {
        if (this.columnSize != 1) {
            throw IllFormSelectError(
                "Recursive SELECT must return just one column, got ${
                    trimListToPrint(
                        this.headers,
                        3
                    )
                }"
            )
        }
        return this.data.asSequence().map { it.first() }
    }

    private fun toOutputForm(data: Any?): String {
        return when (data) {
            is Long -> Date(data).toString()
            is Int -> data.toString()
            is String -> data
            is Float -> data.toString()
            is Double -> data.toString()
            else -> "NULL"
        }
    }

    fun outputTable(): String {
        val prettyTable = PrettyTable(*this.headers.toTypedArray())
        val extraMessage = this.data[0]
        for (line in this.data) {
            prettyTable.addRow(*line.map { toOutputForm(it) }.toTypedArray())
        }
        var table = prettyTable.toString()
        table = table.plus("${this.data.size} Items got.").plus("\n")
        this.extra?.forEach {
            table = table.plus(it).plus("\n")
        }
        return table
    }
}
