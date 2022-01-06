package systemManagement

abstract class Selector(val tableName: String, val columnName: String) {
    companion object {
        const val WILDCARD = "*"
    }
}

class FieldSelector(_tableName: String, _columnName: String) : Selector(_tableName, _columnName) {
}

class CountSelector : Selector(Selector.WILDCARD, Selector.WILDCARD) {
}

class AggregationSelector(
    _tableName: String,
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
