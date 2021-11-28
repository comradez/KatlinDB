package utils

import kotlin.experimental.and
typealias BufferType = ByteArray
typealias ErrorCode = Int
typealias RID = Pair<Int, Int> // (pageId, slotId)

const val PAGE_NUMBER = 60000 // 缓冲区内总页数
const val MAX_FILE_NUMBER = 128 // 支持同时打开的文件个数
const val PAGE_SHIFT = 8 // 页面大小的 log2 值
const val PAGE_SIZE = 1 shl PAGE_SHIFT // 页面大小
const val PAGE_HEADER_SIZE = 64 // 每页的页头大小

/**
 * @brief 把 32 位整数以*小端序*格式写入字节数组的指定位置
 * @param data 待写入 32 位整数
 * @param array 目标字节数组
 * @param offset 字节数组写入位置
 */
fun writeIntToByteArray(data: Int, array: ByteArray, offset: Int = 0) {
    array[offset] = (data and 0xFF).toByte()
    array[offset + 1] = ((data shr 8) and 0xFF).toByte()
    array[offset + 2] = ((data shr 16) and 0xFF).toByte()
    array[offset + 3] = ((data shr 24) and 0xFF).toByte()
}

/**
 * @brief 从字节数组的指定位置开始读取一个 32 位整数
 * @param array 源字节数组
 * @param offset 字节数组读取位置
 * @return 读取到的 32 位整数
 */
fun readIntFromByteArray(array: ByteArray, offset: Int = 0): Int {
    var data = 0
    data += array[offset].toInt()
    data += array[offset + 1].toInt() shl 8
    data += array[offset + 2].toInt() shl 16
    data += array[offset + 3].toInt() shl 24
    return data
}

/**
 * @brief 返回指定 Byte 二进制表示中 1 的个数
 * @param data 输入 Byte
 * @return 1 的个数
 */
fun popCount(data: Byte): Int {
    var ret = 0
    ret += data and 0x01
    ret += if (data and 0x02 != 0.toByte()) { 1 } else { 0 }
    ret += if (data and 0x04 != 0.toByte()) { 1 } else { 0 }
    ret += if (data and 0x08 != 0.toByte()) { 1 } else { 0 }
    ret += if (data and 0x10 != 0.toByte()) { 1 } else { 0 }
    ret += if (data and 0x20 != 0.toByte()) { 1 } else { 0 }
    ret += if (data and 0x40 != 0.toByte()) { 1 } else { 0 }
    ret += if (data.toInt() and 0x80 != 0) { 1 } else { 0 }
    return ret
}

fun firstZero(data: Byte): Int {
    val uData = data.toUByte()
    if (uData < 128U) return 0
    if (uData < 192U) return 1
    if (uData < 224U) return 2
    if (uData < 240U) return 3
    if (uData < 248U) return 4
    if (uData < 252U) return 5
    if (uData < 254U) return 6
    if (uData < 255U) return 7
    return -1
}

fun getOccupiedMask(index: Int): Byte {
    return (1 shl (7 - index % 8)).toByte()
}

fun getVacantMask(index: Int): Byte {
    return when (7 - index % 8) {
        0 -> 254
        1 -> 253
        2 -> 251
        3 -> 247
        4 -> 239
        5 -> 223
        6 -> 191
        else -> 127
    }.toByte()
}

fun getOnes(data: Byte): List<Int> {
    val uData = data.toUByte()
    return (0 .. 7)
        .filter { uData and (1 shl (7 - it)).toUByte() != 0.toUByte() }
        .toList()
}