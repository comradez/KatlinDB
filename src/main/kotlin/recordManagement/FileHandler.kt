package recordManagement

import com.github.michaelbull.result.*
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlin.experimental.and
import kotlin.experimental.or
import pagedFile.BufferManager
import utils.*
import java.io.File

class FileHandler(_file: File, _bufferManager: BufferManager) {
    val file = _file
    private val bufferManager = _bufferManager
    val config: HeaderPageConfig
    var configChanged = false

    init {
        val headerPage = bufferManager.readPage(file, 0)
        val headerString = headerPage
            .decodeToString()
            .trim { it == 0.toChar() }
        config = Json.decodeFromString(headerString)
    }

    /**
     * @brief 标记某个 RID 被占用
     * @param rid 待标记的 RID
     */
    private fun markOccupied(rid: RID) {
        val (pageId, slotId) = rid
        val page = bufferManager.readPage(file, pageId)
        val offset = slotId / 8
        page[offset] = page[offset] or getOccupiedMask(slotId)
    }

    /**
     * @brief 标记某个 RID 是空闲的
     * @param rid 待标记的 RID
     */
    private fun markVacant(rid: RID) {
        val (pageId, slotId) = rid
        val page = bufferManager.readPage(file, pageId)
        val offset = slotId / 8
        page[offset] = page[offset] and getVacantMask(slotId)
    }

    /**
     * @brief 检查某个 RID 是否空闲
     * @param rid RID
     * @return 如果这个位置空闲（可以放新条目）为 true，否则为 false
     */
    private fun checkVacancy(rid: RID): Boolean {
        val (pageId, slotId) = rid
        val page = bufferManager.readPage(file, pageId)
        val offset = slotId / 8
        return page[offset] or getOccupiedMask(slotId) == 0.toByte()
    }

    /**
     * @brief 更新指定页的“下一空闲页”指针
     * @param pageId 待更新的页编号
     * @param newNextIdlePage 新的“下一空闲页”编号
     */
    private fun updateNextIdlePage(pageId: Int, newNextIdlePage: Int) {
        val page = bufferManager.readPage(file, pageId)
        writeIntToByteArray(newNextIdlePage, page, config.bitMapLength)
        bufferManager.markDirty(file, pageId)
    }

    /**
     * @brief 检查指定页面是否满
     * @param pageId 页编号
     * @return 如页满，为 true；否则为 false
     */
    private fun checkPageFull(pageId: Int): Boolean {
        val bitMap = bufferManager
            .readPage(file, pageId) // 取出 page
            .sliceArray(0 until config.bitMapLength) // 取出 BitMap 部分
        val firstZeroPos = firstZero(bitMap.last())
        val offset = if (firstZeroPos >= 0) { firstZeroPos } else { 8 }
        return 8 * (bitMap.size - 1) + offset == config.slotPerPage
    }

    /**
     * @brief 获得指定页面中第一条空闲的 RID
     * @param pageId 页编号
     * @return 返回一个 Result ，如果页面中存在空闲槽则里面为 Ok(RID)，否则为 Err
     */
    private fun firstVacantSlot(pageId: Int): Result<RID, Unit> {
        val offset = bufferManager
            .readPage(file, pageId)
            .sliceArray(0 until config.bitMapLength)
            .indexOfFirst { it != 0xFF.toByte() }
        val page = bufferManager.readPage(file, pageId)
        return if (offset != -1)
            Ok(RID(pageId, offset * 8 + firstZero(page[offset])))
        else
            Err(Unit)
    }

    /**
     * 根据指定 [RID] 获取一条记录的字节序列，
     * 返回的是拷贝，无副作用
     * @param rid (页号, 槽号)，用于标识一条记录
     */
    fun getRecord(rid: RID): Record {
        val (pageId, slotId) = rid
        val page = bufferManager.readPage(file, pageId)
        val offset = PAGE_HEADER_SIZE + config.recordLength * slotId
        val buffer = ByteArray(config.recordLength)
        page.copyInto(buffer, 0, offset, offset + config.recordLength)
        return Record(rid, buffer)
    }

    /**
     * @brief 根据指定 RID 删除一条记录
     * @param rid (页号, 槽号)，用于标识一条记录
     * @return 返回一个 Result，如成功无返回值，如失败为内部为错误状态码
     */
    fun deleteRecord(rid: RID): Result<Unit, ErrorCode> {
        val pageId = rid.first
        if (checkPageFull(pageId)) { // 这个页面之前满，现在不满了，插入到未满页面链表头
            updateNextIdlePage(pageId, config.nextAvailablePage)
            config.nextAvailablePage = pageId
        }
        markVacant(rid) // 要更新位图，但*不一定*要把这条记录抹成0
        config.recordNumber -= 1 // 记录条数 - 1
        configChanged = true // header 出现修改，标记写回
        bufferManager.markDirty(file, pageId)
        return Ok(Unit)
    }

    /**
     * @brief 获得下一条可用的 RID
     * @return RID
     */
    private fun nextAvailableRID(): RID {
//        println("nextAvailablePage is ${config.nextAvailablePage}")
        return if (config.nextAvailablePage == config.pageNumber) { // 所有已有页面都分配完了
            RID(createPage(), 0)
        } else {
            checkNotNull(firstVacantSlot(config.nextAvailablePage).get())
        }
    }

    /**
     * @brief 在整张表的所有页都满之后，创建一个新页并对应调整空闲页链表
     * @return 新页的 pageId
     */
    private fun createPage(): Int {
        val pageId = config.pageNumber
        config.pageNumber += 1
//        println("create page $pageId, nextAvailablePage is ${config.nextAvailablePage}, there are ${config.pageNumber} pages.")
        if (pageId != config.nextAvailablePage) { // 前面有其他的未满页面
            updateNextIdlePage(pageId, config.nextAvailablePage)
            config.nextAvailablePage = pageId
            // 把当前页面插入到链表头
        } else { // 所有前面的页面都已经填满了
            updateNextIdlePage(pageId, config.pageNumber)
//            config.nextAvailablePage
            // 不用对链表做额外修改，只需要将自己的 nextPage 指向下一个可分配页号即可
        }
        configChanged = true
        bufferManager.markDirty(file, pageId)
        return pageId
    }

    /**
     * 插入一条记录
     * @param buffer 记录的字节序列
     * @return 返回代表插入位置的 [RID]
     */
    fun insertRecord(buffer: BufferType): RID {
        val rid = nextAvailableRID()
        val (pageId, slotId) = rid
//        println("file is ${file.name}, recordLength is ${config.recordLength}, pageId is $pageId, slotId is $slotId")
        markOccupied(rid)
        val page = bufferManager.readPage(file, pageId)
        if (checkPageFull(pageId)) { // 之前不满，现在满了
            config.nextAvailablePage = readIntFromByteArray(page, config.bitMapLength)
//            println("page full! nextAvailablePage is ${config.nextAvailablePage}")
            configChanged = true
        }
        val offset = PAGE_HEADER_SIZE + config.recordLength * slotId
        buffer.copyInto(page, offset, 0, config.recordLength)
        bufferManager.markDirty(file, pageId)
        return rid
    }

    /**
     * @brief 更新指定 RID 处的记录，将其改为 buffer
     * @param rid (页号, 槽号)，用于标识一条记录
     * @param buffer 记录的字节序列
     * @return 返回一个 Result，如成功无返回值，如失败为内部为错误状态码
     * @remark 如果它此处原本没有记录则插入失败，因为我要维护空闲页面链表。所以 update 操作不会对空闲状况产生任何影响
     */
    fun updateRecord(rid: RID, buffer: ByteArray): Result<Unit, ErrorCode> {
        if (checkVacancy(rid)) { // 如果此处原本没有记录
            return Err(2) // 插入失败
        }
        val (pageId, slotId) = rid
        val page = bufferManager.readPage(file, pageId)
        val offset = PAGE_HEADER_SIZE + config.recordLength * slotId
        buffer.copyInto(page, offset, 0, config.recordLength)
        bufferManager.markDirty(file, pageId)
        return Ok(Unit)
    }

    /**
     * @brief 给定页数，返回该页内所有非空记录的 RID
     * @param pageId 页号
     * @return 包含该页中所有非空记录 RID 的列表
     */
    private fun getItemRIDsForPage(pageId: Int) = bufferManager
        .readPage(file, pageId).asSequence()
        .take(config.bitMapLength)
        .map { getOnes(it) }
        .flatMapIndexed { i, list -> list.map { it + 8 * i } }
        .map { RID(pageId, it) }

    /**
     * @brief 返回表内所有非空记录的 RID
     * @return 包含该表中所有非空记录 RID 的列表
     */
    fun getItemRIDs() = (1 until config.pageNumber).asSequence() // Page 0 是表头
        .flatMap { getItemRIDsForPage(it) }
}
