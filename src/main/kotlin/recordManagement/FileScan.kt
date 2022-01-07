package recordManagement

import com.github.michaelbull.result.get
import utils.PAGE_SIZE
import java.nio.ByteBuffer
import java.nio.ByteOrder

class FileScan {
    fun openScan(
        fileHandler: FileHandler,
        attrType: AttributeType,
        attrLength: Int?,
        attrOffset: Int?,
        // 如果两个都给了非 null 值那么选取 [attrOffset, attrOffset + attrLength)
        // 只要有一个 null 那么选取的是整个 Record 的范围，即 [0, PAGE_SIZE)
        compareOp: CompareOp,
        value: Any? // 可能是 String, Int 或者 Float
    ) = sequence {
            val availableRIDs = fileHandler.getItemRIDs().iterator()
            while (availableRIDs.hasNext()) {
                val record = checkNotNull(fileHandler.getRecord(availableRIDs.next()).get())
                val requiredAttrBytes = record.data.sliceArray(if (attrOffset != null && attrLength != null) {
                    attrOffset until attrOffset + attrLength
                } else {
                    0 until PAGE_SIZE
                })
                when (attrType) {
                    AttributeType.INT -> {
                        val requiredAttr = ByteBuffer.wrap(requiredAttrBytes).order(ByteOrder.LITTLE_ENDIAN).int
                        if (comp(requiredAttr, value as Int?, compareOp)) { // 这里改成比较函数
                            yield(record)
                        }
                    }
                    AttributeType.FLOAT -> {
                        val requiredAttr = ByteBuffer.wrap(requiredAttrBytes).order(ByteOrder.LITTLE_ENDIAN).float
                        if (comp(requiredAttr, value as Float?, compareOp)) {
                            yield(record)
                        }
                    }
                    AttributeType.STRING -> {
                        val requiredAttr = requiredAttrBytes.decodeToString()
                        if (strComp(requiredAttr, value as String?, compareOp)) {
                            yield(record)
                        }
                    }
                    AttributeType.LONG -> {
                        val requiredAttr = ByteBuffer.wrap(requiredAttrBytes).order(ByteOrder.LITTLE_ENDIAN).long
                        if (comp(requiredAttr, value as Long?, compareOp)) {
                            yield(record)
                        }
                    }
                }
            }
        }

    private fun<T: Comparable<T>> comp(first: T, second: T?, op: CompareOp): Boolean {
        return if (second != null) {
            when (op) {
                CompareOp.GT_OP -> first > second
                CompareOp.LT_OP -> first < second
                CompareOp.GE_OP -> first >= second
                CompareOp.LE_OP -> first <= second
                CompareOp.EQ_OP -> first == second
                CompareOp.NE_OP -> first != second
                CompareOp.NO_OP -> true
            }
        } else { true }
    }


    private fun strComp(first: String, second: String?, op: CompareOp): Boolean {
        return if (second != null) {
            when (op) {
                CompareOp.EQ_OP -> first == second
                CompareOp.NE_OP -> first != second
                else -> true // 字符串只能比较相等或不等，输入其他 op 视同 NO_OP
            }
        } else { true }
    }
}