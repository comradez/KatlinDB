package parser

data class QueryResult(
    val headers: List<String>,
    val data: List<List<String>>
)