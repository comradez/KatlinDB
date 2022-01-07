package parser

abstract class QueryResult(
    val headers: List<String>?,
    val data: List<List<String>>?
) {
    var timeCost: Long? = null
}

class SuccessResult(
    headers: List<String>?,
    data: List<List<String>>?
) : QueryResult(headers, data)

class EmptyResult() : QueryResult(null, null)

class ErrorResult(val errorMessage: String) : QueryResult(null, null)