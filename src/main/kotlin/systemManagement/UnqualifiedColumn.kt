package systemManagement

import java.util.*

data class UnqualifiedColumn(var tableName: String?, val columnName: String) {
    override fun toString(): String =
        if (this.tableName == null) {
            this.columnName
        } else {
            "${this.tableName}.${this.columnName}"
        }

    override fun equals(other: Any?): Boolean =
        when (other) {
            is UnqualifiedColumn -> this.tableName == other.tableName &&
                    this.columnName == other.columnName
            else -> false
        }

    override fun hashCode(): Int = Objects.hash(this.tableName, this.columnName)
}
