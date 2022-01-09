package systemManagement

import parser.SuccessResult
import systemManagement.AggregationSelector.Aggregator
import utils.TypeMismatchError

/**
 * 认为 null 在除 COUNT 外的 aggregation 中不做贡献
 */
fun aggregate(
    aggregator: Aggregator,
    column: List<Any?>
): Any? {
    if (aggregator == Aggregator.COUNT) {
        return column.size
    }
    val column = column.filterNotNull()
    if (column.isEmpty()) {
        return null
    }
    return when (aggregator) {
        Aggregator.AVERAGE -> when (val first = column.first()) {
            is Int -> column.sumOf { it as Int } / column.size.toDouble()
            is Long -> column.sumOf { it as Long } / column.size.toDouble()
            is Float -> column.sumOf { (it as Float).toDouble() } / column.size
            else -> throw TypeMismatchError(first)
        }
        Aggregator.MAX -> when (val first = column.first()) {
            is Int -> column.maxOf { it as Int }
            is Long -> column.maxOf { it as Long }
            is Float -> column.maxOf { it as Float }
            is String -> column.maxOf { it as String }
            else -> error(first)
        }
        Aggregator.MIN -> when (val first = column.first()) {
            is Int -> column.minOf { it as Int }
            is Long -> column.minOf { it as Long }
            is Float -> column.minOf { it as Float }
            is String -> column.minOf { it as String }
            else -> error(first)
        }
        Aggregator.SUM -> when (val first = column.first()) {
            is Int -> column.sumOf { it as Int }
            is Long -> column.sumOf { it as Long }
            is Float -> column.sumOf { (it as Float).toDouble() }
            is String -> column.reduce { acc, ele -> acc as String + ele as String }
            else -> error(first)
        }
        else -> error(aggregator)
    }
}

fun aggregate(
    headers: List<Pair<Selector, Int>>,
    groups: Collection<List<List<Any?>>>
): SuccessResult {
    val result = groups.map { group ->
        headers.map { (selector, index) ->
            val column = group.map { row -> row[index] }
            when (selector) {
                is WildcardCountSelector -> column.size
                is AggregationSelector -> aggregate(selector.aggregator, column)
                else /* FieldSelector */ -> column.first()
            }
        }
    }
    return SuccessResult(headers.map { (selector, _) -> selector.toString() }, result)
}
