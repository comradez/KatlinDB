package systemManagement

import utils.InternalError

abstract class Selector(_tableName: String?, _columnName: String) {
    val column = UnqualifiedColumn(_tableName, _columnName)
    val tableName get() = this.column.tableName
    val columnName get() = this.column.columnName

    companion object {
        const val WILDCARD = "*"
    }

    operator fun component1() = this.tableName
    operator fun component2() = this.columnName
}

class FieldSelector(_tableName: String?, _columnName: String) : Selector(_tableName, _columnName) {
}

class CountSelector : Selector(WILDCARD, WILDCARD) {
}

class AggregationSelector(
    _tableName: String?,
    _columnName: String,
    val aggregator: Aggregator
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
