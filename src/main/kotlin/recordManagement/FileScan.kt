package recordManagement

import com.github.michaelbull.result.get
import java.nio.ByteBuffer
import java.nio.ByteOrder

class FileScan {
    fun openScan(
        fileHandler: FileHandler,
        attrType: AttributeType,
        attrLength: Int,
        attrOffset: Int,
        compareOp: CompareOp,
        value: Any // 可能是 String, Int 或者 Float
    ) = sequence {
            val availableRIDs = fileHandler.getItemRIDs().iterator()
            while (availableRIDs.hasNext()) {
                val record = checkNotNull(fileHandler.getRecord(availableRIDs.next()).get())
                val requiredAttrBytes = record.data.sliceArray(attrOffset until attrOffset + attrLength)
                when (attrType) {
                    AttributeType.INT -> {
                        val requiredAttr = ByteBuffer.wrap(requiredAttrBytes).order(ByteOrder.LITTLE_ENDIAN).int
                        if (comp(requiredAttr, value as Int, compareOp)) { // 这里改成比较函数
                            yield(record)
                        }
                    }
                    AttributeType.FLOAT -> {
                        val requiredAttr = ByteBuffer.wrap(requiredAttrBytes).order(ByteOrder.LITTLE_ENDIAN).float
                        if (comp(requiredAttr, value as Float, compareOp)) {
                            yield(record)
                        }
                    }
                    AttributeType.STRING -> {
                        val requiredAttr = requiredAttrBytes.decodeToString()
                        if (comp(requiredAttr, value as String, compareOp)) {
                            yield(record)
                        }
                    }
                }
            }
        }

    private fun comp(first: Int, second: Int, op: CompareOp): Boolean {
        return when (op) {
            CompareOp.GT_OP -> first > second
            CompareOp.LT_OP -> first < second
            CompareOp.GE_OP -> first >= second
            CompareOp.LE_OP -> first <= second
            CompareOp.EQ_OP -> first == second
            CompareOp.NE_OP -> first != second
            CompareOp.NO_OP -> true
        }
    }

    private fun comp(first: Float, second: Float, op: CompareOp): Boolean {
        return when (op) {
            CompareOp.GT_OP -> first > second
            CompareOp.LT_OP -> first < second
            CompareOp.GE_OP -> first >= second
            CompareOp.LE_OP -> first <= second
            CompareOp.EQ_OP -> first == second
            CompareOp.NE_OP -> first != second
            CompareOp.NO_OP -> true
        }
    }

    private fun comp(first: String, second: String, op: CompareOp): Boolean {
        return when (op) {
            CompareOp.EQ_OP -> first == second
            CompareOp.NE_OP -> first != second
            else -> true // 字符串只能比较相等或不等，输入其他 op 视同 NO_OP
        }
    }
}