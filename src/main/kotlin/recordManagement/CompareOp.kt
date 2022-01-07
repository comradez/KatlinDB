package recordManagement

enum class CompareOp {
    EQ_OP, // 相等
    LT_OP, // 小于
    GT_OP, // 大于
    LE_OP, // 小于等于
    GE_OP, // 大于等于
    NE_OP, // 不等于
    NO_OP  // 无操作
}

fun buildCompareOp(compareOp: String): CompareOp {
    return when (compareOp) {
        "=" -> CompareOp.EQ_OP
        "<"  -> CompareOp.LT_OP
        ">"  -> CompareOp.GT_OP
        "<=" -> CompareOp.LE_OP
        ">=" -> CompareOp.GE_OP
        "<>" -> CompareOp.NE_OP
        else -> CompareOp.NO_OP
    }
}