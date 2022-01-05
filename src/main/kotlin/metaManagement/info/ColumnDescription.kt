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
}