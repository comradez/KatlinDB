package dataConverter

import com.github.michaelbull.result.zip
import recordManagement.AttributeType
import recordManagement.Record
import utils.*

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
                    AttributeType.LONG -> writeLongToByteArray(value as Long? ?: Long.MIN_VALUE, record, offset)
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
                        AttributeType.STRING -> readStringFromByteArray(data, offset = offset, size = size).takeUnless { it.isEmpty() }
                        AttributeType.LONG -> readLongFromByteArray(data, offset).takeUnless { it == Long.MIN_VALUE }
                    }
                )
                offset += size
            }
            assert(offset == totalSize)
            return list
        }
    }
}
