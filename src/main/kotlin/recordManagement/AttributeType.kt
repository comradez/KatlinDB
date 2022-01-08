package recordManagement

import utils.InternalError

enum class AttributeType {
    LONG, // 8 字节整数
    INT, // 4 字节整数
    FLOAT, // 4 字节浮点数
    STRING // VARCHAR
}

fun buildAttributeType(type: String): AttributeType {
    return when (type.uppercase()) {
        "LONG" -> AttributeType.LONG
        "INT" -> AttributeType.INT
        "FLOAT" -> AttributeType.FLOAT
        "VARCHAR" -> AttributeType.STRING
        else -> throw InternalError("Invalid type literal $type.")
    }
}