package systemManagement

import java.util.*

interface Selector {
    companion object {
        const val WILDCARD = "*"
    }

    override fun equals(other: Any?): Boolean
    override fun hashCode(): Int
}

open class WildcardSelector : Selector {
    override fun equals(other: Any?): Boolean = other is WildcardSelector
    override fun hashCode(): Int = javaClass.hashCode()
}

class WildcardCountSelector : Selector {
    override fun equals(other: Any?): Boolean = other is WildcardCountSelector
    override fun hashCode(): Int = javaClass.hashCode()
    override fun toString(): String = "Count"
}

open class FieldSelector(val column: UnqualifiedColumn) : Selector {
    val tableName get() = this.column.tableName
    val columnName get() = this.column.columnName

    operator fun component1() = this.tableName
    operator fun component2() = this.columnName

    override fun equals(other: Any?): Boolean =
        when (other) {
            is FieldSelector -> this.column == other.column
            else -> false
        }

    override fun hashCode(): Int = this.column.hashCode()

    override fun toString(): String = this.column.toString()
}

class AggregationSelector(
    val column: UnqualifiedColumn,
    val aggregator: Aggregator
) : Selector {
    enum class Aggregator {
        AVERAGE,
        COUNT,
        MAX,
        MIN,
        SUM
    }

    override fun equals(other: Any?): Boolean = when (other) {
        is AggregationSelector -> this.column == other.column && this.aggregator == other.aggregator
        else -> false
    }

    override fun hashCode(): Int = Objects.hash(this.column, this.aggregator)

    override fun toString(): String = this.column.toString()
}
