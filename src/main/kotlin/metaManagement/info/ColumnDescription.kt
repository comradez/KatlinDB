package metaManagement.info

import recordManagement.AttributeType

data class ColumnDescription(
    val name: String,
    val type: Pair<AttributeType, Int?>, // 类型，长度（长度是用于 String 的）
    val isNull: Boolean,
    val default: Any
) {
    var key: String? = null
    var extra: String? = null

    companion object {
        val keys: List<String> = listOf("Field", "Type", "Null", "Key", "Default", "Extra")
    }

    val values: List<String>
        get() = listOf(
            this.name, // field
            when (this.type.first) {
                AttributeType.STRING -> "VARCHAR(${this.type.second!!})"
                else -> this.type.first.name
            }, // type
            if (this.isNull) "YES" else "NO", // null
            (this.key ?: ""), // key
            this.default.toString(), // default
            (this.extra ?: "") // extra
        )
}
