package recordManagement

import com.github.michaelbull.result.Ok
import com.github.michaelbull.result.Result
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import pagedFile.BufferManager
import utils.PAGE_HEADER_SIZE
import utils.PAGE_SIZE

typealias ErrorCode = Int

class RecordHandler {
    private val bufferManager = BufferManager()
    private val openedFiles = mutableListOf<FileHandler>()

    /**
     * @brief 依据文件名创建指定记录长度的文件
     * @param fileName 文件名
     * @param recordLength 记录长度
     * @return 是否创建成功
     * @remark 创建时会打开文件并且写入页头
     */
    fun createFile(fileName: String, recordLength: Int): Boolean {
        if (bufferManager.createFile(fileName)) {
            val fileCreated = bufferManager.openFile(fileName)
            val headerPage = bufferManager.readPage(fileCreated, 0)
            val slotPerPage = acquireSlotPerPage(recordLength)
            val bitMapLength = acquireBitMapLength(slotPerPage)
            assert(bitMapLength + 4 <= PAGE_HEADER_SIZE) // 确保这东西 header 装得下
            val headerPageConfig = HeaderPageConfig(
                recordLength,
                slotPerPage,
                0,
                1,
                1,
                bitMapLength
            ) // 创建头部元信息页
            Json.encodeToString(headerPageConfig)
                .toByteArray()
                .copyInto(headerPage) // 将元信息写入头部页
            bufferManager.markDirty(fileCreated, 0) // 标记写回
            bufferManager.closeFile(fileCreated)
            return true
        }
        return false
    }

    /**
     * @brief 计算每页的记录数
     * @param recordLength 每条记录的长度
     * @return 每页的记录数
     */
    private fun acquireSlotPerPage(recordLength: Int): Int {
        return (PAGE_SIZE - PAGE_HEADER_SIZE) / recordLength
    }

    /**
     * @brief 计算位图字节数
     * @param slotPerPage 每页的记录数
     * @return 位图字节数
     */
    private fun acquireBitMapLength(slotPerPage: Int): Int {
        return (slotPerPage + 7) shr 3
    }

    /**
     * @brief 依据文件名删除文件
     * @param fileName 文件名
     * @return 是否删除成功
     */
    fun removeFile(fileName: String): Boolean {
        return bufferManager.removeFile(fileName)
    }

    /**
     * @brief 根据文件名打开文件，返回 FileHandler
     * @param fileName 文件名
     * @return 返回 Result，如果成功为处理该文件的 FileHandler，失败则为错误码
     */
    fun openFile(fileName: String): Result<FileHandler, ErrorCode> {
        val file = bufferManager.openFile(fileName)
        val fileHandler = FileHandler(file, bufferManager)
        openedFiles.add(fileHandler)
        return Ok(fileHandler)
    }

    /**
     * @brief 根据 FileHandler 关闭文件
     * @param fileHandler 待关闭文件的 Handler
     * @return 是否关闭成功
     */
    fun closeFile(fileHandler: FileHandler): Boolean {
        val file = fileHandler.file
        if (fileHandler.configChanged) {
            bufferManager.markDirty(file, 0)
        } // 标记表头写回
        bufferManager.closeFile(file)
        return true
    }
}
