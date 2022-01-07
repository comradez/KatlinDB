package systemManagement

import utils.InternalError

abstract class Selector(val tableName: String?, val columnName: String) {
    companion object {
        const val WILDCARD = "*"
    }
}

class FieldSelector(_tableName: String?, _columnName: String) : Selector(_tableName, _columnName) {
}

class CountSelector : Selector(Selector.WILDCARD, Selector.WILDCARD) {
}

class AggregationSelector(
    _tableName: String?,
    _columnName: String,
    aggregator: Aggregator
) : Selector(_tableName, _columnName) {
    enum class Aggregator {
        AVERAGE,
        COUNT,
        MAX,
        MIN,
        SUM
    }
}

fun buildAggregator(aggregator: String): AggregationSelector.Aggregator {
    return when (aggregator.uppercase()) {
        "AVERAGE" -> AggregationSelector.Aggregator.AVERAGE
        "COUNT" -> AggregationSelector.Aggregator.COUNT
        "MAX" -> AggregationSelector.Aggregator.MAX
        "MIN" -> AggregationSelector.Aggregator.MIN
        "SUM" -> AggregationSelector.Aggregator.SUM
        else -> throw InternalError("Bad aggregator type")
    }
}