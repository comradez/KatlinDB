package systemManagement

import parser.SuccessResult

/**
 * [_outerJoinedColumns] 和 [_innerJoinedColumns] 都是 qualified 的 column name
 */
fun nestedLoopJoin(
    _outerResult: SuccessResult,
    _innerResult: SuccessResult,
    _outerJoinedColumns: List<String>,
    _innerJoinedColumns: List<String>
): SuccessResult {
    assert(_outerJoinedColumns.size == _innerJoinedColumns.size)
    var outerResult = _outerResult
    var innerResult = _innerResult
    var outerJoinedColumns = _outerJoinedColumns
    var innerJoinedColumns = _innerJoinedColumns
    if (outerResult.rowSize < innerResult.rowSize) {
        val (outerResult_, innerResult_) = Pair(innerResult, outerResult)
        outerResult = outerResult_; innerResult = innerResult_
        val (outerJoinedColumns_, innerJoinedColumns_) = Pair(
            innerJoinedColumns,
            outerJoinedColumns
        )
        outerJoinedColumns = outerJoinedColumns_
        innerJoinedColumns = innerJoinedColumns_
    }
    val outerJoinedColumnIndices = outerJoinedColumns.map { columnName ->
        outerResult.getHeaderIndex(columnName)
    }
    val innerJoinedColumnIndices = innerJoinedColumns.map { columnName ->
        innerResult.getHeaderIndex(columnName)
    }
    val outerLeftColumnIndices = outerResult.columnIndices
        .subtract(outerJoinedColumnIndices.toSet())
    val innerLeftColumnIndices = innerResult.columnIndices
        .subtract(innerJoinedColumnIndices.toSet())
    // joined columns => row indices
    val outerRowIndices = mutableMapOf<List<Any?>, MutableList<Int>>()
    outerResult.data.asSequence()
        .map { row -> outerJoinedColumnIndices.map { row[it] } }
        .forEachIndexed { i, row ->
            outerRowIndices.getOrPut(row) { mutableListOf() }.add(i)
        }
    val result = mutableListOf<List<Any?>>()
    innerResult.data.asSequence().forEach { innerRow ->
        val joined = innerJoinedColumnIndices.map { innerRow[it] }
        outerRowIndices[joined]?.forEach { outerRowIndex ->
            val outerLeft = outerLeftColumnIndices.asSequence().map {
                outerResult.data[outerRowIndex][it]
            }
            val innerLeft = innerLeftColumnIndices.asSequence().map { innerRow[it] }
            val row = outerLeft + innerLeft + joined
            result.add(row.toList())
        }
    }
    val headers = outerLeftColumnIndices.asSequence().map { outerResult.headers[it] } +
            innerLeftColumnIndices.asSequence().map { innerResult.headers[it] } +
            outerJoinedColumns
    val queryResult = SuccessResult(headers.toList(), result)
    queryResult.setAliases(innerJoinedColumns.asSequence().zip(outerJoinedColumns.asSequence()))
    queryResult.setAliases(outerResult.aliases.asSequence().map { (k, v) -> k to v })
    queryResult.setAliases(innerResult.aliases.asSequence().map { (k, v) -> k to v })
    return queryResult
}
