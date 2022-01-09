package dataConverter

import recordManagement.AttributeType
import recordManagement.Record
import utils.*
import java.sql.Date

class Converter {
    companion object Static {
        fun encode(
            sizeList: List<Int>,
            typeList: List<AttributeType>,
            totalSize: Int,
            valueList: List<Any?>
        ): ByteArray {
            if (sizeList.size != typeList.size || typeList.size != valueList.size) {
                throw InternalError("Lengths of values mismatch with those of columns.")
            }
            val record = ByteArray(totalSize)
            var offset = 0
            for (triple in sizeList zip typeList zip valueList) {
                val size = triple.first.first
                val type = triple.first.second
                val value = triple.second
                when (type) {
                    AttributeType.INT -> writeIntToByteArray(value as Int? ?: Int.MIN_VALUE, record, offset)
                    AttributeType.FLOAT -> writeFloatToByteArray(value as Float? ?: Float.NaN, record, offset)
                    AttributeType.STRING -> writeStringToByteArray(value as String? ?: "", record, offset)
                    AttributeType.LONG -> writeLongToByteArray(
                        if (value != null) { Date.valueOf(value as String).time } else { Long.MIN_VALUE },
                        record,
                        offset
                    )
                }
                offset += size
            }
            assert(offset == totalSize)
            return record
        }

        fun decode(sizeList: List<Int>, typeList: List<AttributeType>, totalSize: Int, record: Record): List<Any?> {
            val data = record.data
            val list = mutableListOf<Any?>()
            var offset = 0
            for ((size, type) in sizeList zip typeList) {
                list.add(
                    when (type) {
                        AttributeType.INT -> readIntFromByteArray(data, offset).takeUnless { it == Int.MIN_VALUE }
                        AttributeType.FLOAT -> readFloatFromByteArray(data, offset).takeUnless { it.isNaN() }
                        AttributeType.STRING -> readStringFromByteArray(data, offset, size).takeUnless { it.isEmpty() }
                        AttributeType.LONG -> readLongFromByteArray(data, offset).takeUnless { it == Long.MIN_VALUE }
                    }
                )
                offset += size
            }
            assert(offset == totalSize)
            return list
        }

        fun convertFromString(input: String, type: AttributeType): Any {
            return when (type) {
                AttributeType.INT -> input.toInt()
                AttributeType.FLOAT -> input.toFloat()
                AttributeType.STRING -> input
                AttributeType.LONG -> Date.valueOf(input).time
            }
        }

        fun convertToString(value: Any?, type: AttributeType): String {
            return when (type) {
                AttributeType.INT -> (value as Int?)?.toString() ?: "NULL"
                AttributeType.FLOAT -> if (value != null) { "%.2f".format(value as Float) } else { "NULL" }
                AttributeType.STRING -> (value as String?) ?: "NULL"
                AttributeType.LONG -> if (value != null) { Date(value as Long).toString() } else { "NULL" }
            }
        }
    }
}
