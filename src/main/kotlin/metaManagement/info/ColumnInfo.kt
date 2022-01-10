package metaManagement.info

import kotlinx.serialization.Serializable
import recordManagement.AttributeType

@Serializable
class ColumnInfo(
    val type: AttributeType,
    val name: String,
    val size: Int,
    var nullable: Boolean = true
) {
    fun getColumnSize() : Int {
        return when (type) {
            AttributeType.INT -> 4
            AttributeType.FLOAT -> 4
            AttributeType.STRING -> size
            AttributeType.LONG -> 8
        }
    }

    fun getDescription(): ColumnDescription {
        val typePair = type to if (type == AttributeType.STRING) { size } else { null }
        val default: Any = when (type) {
            AttributeType.INT, AttributeType.LONG -> 0
            AttributeType.FLOAT -> 0.0
            AttributeType.STRING -> ""
        }
        return ColumnDescription(name, typePair, nullable, default)
    }
}
