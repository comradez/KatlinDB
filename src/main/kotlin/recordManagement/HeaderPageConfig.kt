package recordManagement

import kotlinx.serialization.Serializable

@Serializable
data class HeaderPageConfig(
    val recordLength: Int, // 记录长度，固定
    val slotPerPage: Int, // 一页可存放的最大记录数，固定
    var recordNumber: Int, // 表中的记录总数，可变
    var pageNumber: Int, // 表中的总页数，可变
    var nextAvailablePage: Int, // 下一个有空闲的页的编号，可变
    val bitMapLength: Int // 每页位图长度（在页头用位图储存每个槽是否为空），固定
)