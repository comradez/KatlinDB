package recordManagement

enum class CompareOp {
    EQ_OP, // 相等
    LT_OP, // 小于
    GT_OP, // 大于
    LE_OP, // 小于等于
    GE_OP, // 大于等于
    NE_OP; // 不等于

    fun map(): (Int?) -> Boolean =
        when (this) {
            EQ_OP -> { result -> result != null && result == 0 }
            LT_OP -> { result -> result != null && result < 0 }
            GT_OP -> { result -> result != null && result > 0 }
            LE_OP -> { result -> result != null && result <= 0 }
            GE_OP -> { result -> result != null && result >= 0 }
            NE_OP -> { result -> result != null && result != 0 }
        }
}
