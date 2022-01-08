package parser

import org.sk.PrettyTable

abstract class QueryResult(
    val headers: List<String>?,
    val data: List<List<String>>?
) {
    var timeCost: Long? = null

    fun outputTable(): String? {
        if (headers == null || data == null) {
            return null
        }
        val prettyTable = PrettyTable(*headers.toTypedArray())
        for (line in data) {
            prettyTable.addRow(*line.toTypedArray())
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