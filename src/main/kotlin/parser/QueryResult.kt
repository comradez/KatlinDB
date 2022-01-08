package parser

import org.sk.PrettyTable

abstract class QueryResult(
    val headers: List<String>?,
    val data: List<List<Any?>>?
) {
    var timeCost: Long? = null
    init {
        if (headers == null) {
            assert(data == null)
        } else {
            for (line in data!!) {
                assert(line.size == headers.size)
            }
        }
    }

    fun outputTable(): String? {
        if (headers == null || data == null) {
            return null
        }
        val prettyTable = PrettyTable(*headers.toTypedArray())
        for (line in data) {
            val lineArray = line.map { it.toString() }.toTypedArray()
            prettyTable.addRow(*lineArray)
        }
        return prettyTable.toString()
    }
}

class SuccessResult(
    headers: List<String>?,
    data: List<List<String>>?
) : QueryResult(headers, data)

class EmptyResult() : QueryResult(null, null)

class ErrorResult(
    val errorMessage: String
) : QueryResult(null, null)