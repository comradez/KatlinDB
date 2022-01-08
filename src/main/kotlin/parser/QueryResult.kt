package parser

import org.sk.PrettyTable
import utils.NestedSelectError
import utils.trimListToPrint

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
            prettyTable.addRow(*line.map { it.toString() }.toTypedArray())
        }
        return prettyTable.toString()
    }
}

class SuccessResult(
    headers: List<String>?,
    data: List<List<Any?>>?
) : QueryResult(headers, data) {
    fun toColumnForOuterSelect(): Sequence<Any?> {
        if (this.headers!!.size != 1) {
            throw NestedSelectError(
                "Recursive SELECT must return just one column, got ${
                    trimListToPrint(
                        this.headers,
                        3
                    )
                }"
            )
        }
        return this.data!!.asSequence().map { it.first() }
    }
}

class EmptyResult() : QueryResult(null, null)

class ErrorResult(
    val errorMessage: String
) : QueryResult(null, null)
