package recordManagement

import utils.BufferType
import utils.RID

data class Record(
    val rid: RID,
    val data: BufferType
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Record

        if (rid != other.rid) return false
        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = rid.hashCode()
        result = 31 * result + data.contentHashCode()
        return result
    }
}
