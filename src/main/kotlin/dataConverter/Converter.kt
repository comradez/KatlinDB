package dataConverter

import recordManagement.AttributeType
import recordManagement.Record
import utils.*

class Converter {
    companion object Static {
        fun encode(
            sizeList: List<Int>,
            typeList: List<AttributeType>,
            totalSize: Int,
            valueList: List<Any>
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
                    AttributeType.INT -> writeIntToByteArray(value as Int, record, offset)
                    AttributeType.FLOAT -> writeFloatToByteArray(value as Float, record, offset)
                    AttributeType.STRING -> writeStringToByteArray(value as String, record, offset)
                    AttributeType.LONG -> writeLongToByteArray(value as Long, record, offset)
                }
                offset += size
            }
            assert(offset == totalSize)
            return record
        }

        fun decode(sizeList: List<Int>, typeList: List<AttributeType>, totalSize: Int, record: Record): List<Any> {
            val data = record.data
            val list = mutableListOf<Any>()
            var offset = 0
            for ((size, type) in sizeList zip typeList) {
                list.add(
                    when (type) {
                        AttributeType.INT -> readIntFromByteArray(data, offset)
                        AttributeType.FLOAT -> readFloatFromByteArray(data, offset)
                        AttributeType.STRING -> readStringFromByteArray(data, offset = offset, size = size)
                        AttributeType.LONG -> readLongFromByteArray(data, offset)
                    }
                )
                offset += size
            }
            assert(offset == totalSize)
            return list
        }
    }
}