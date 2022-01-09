package recordManagement

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import pagedFile.BufferManager
import utils.PAGE_HEADER_SIZE
import utils.PAGE_SIZE
import java.nio.charset.Charset

private typealias Database = MutableMap<String, FileHandler> // table name => record file

/**
 * 更像是 RecordManager，RecordHandler 的工作实际由 [FileHandler] 完成
 */
class RecordHandler(
    private val bufferManager: BufferManager,
    private val workDir: String
) {
    private val databases = mutableMapOf<String, Database>()

    private fun fileName(databaseName: String, tableName: String) =
        "${this.workDir}/${databaseName}/${tableName}.table"

    /**
     * 依据文件名创建指定记录长度的文件，
     * 创建时会写入页头，并直接写回
     * @param fileName 文件名
     * @param recordLength 记录长度
     * @return 处理该文件的 [FileHandler]
     */
    private fun createFile(fileName: String, recordLength: Int): FileHandler {
        if (bufferManager.createFile(fileName)) {
            val fileCreated = bufferManager.openFile(fileName)
            val headerPage = bufferManager.readPage(fileCreated, 0)
            val slotPerPage = acquireSlotPerPage(recordLength)
            val bitMapLength = acquireBitMapLength(slotPerPage)
//            println("File is $fileName, slotNumber is $slotPerPage, bitMapLength is $bitMapLength")
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
            return FileHandler(fileCreated, this.bufferManager)
        }
        error(fileName)
    }

    fun createRecord(databaseName: String, tableName: String, recordLength: Int): FileHandler =
        this.createFile(this.fileName(databaseName, tableName), recordLength).also {
            this.databases.getOrPut(databaseName) { mutableMapOf() }[tableName] = it
        }

    /**
     * 计算每页的记录数
     * @param recordLength 每条记录的长度
     * @return 每页的记录数
     */
    private fun acquireSlotPerPage(recordLength: Int): Int {
        return (PAGE_SIZE - PAGE_HEADER_SIZE) / recordLength
    }

    /**
     * 计算位图字节数
     * @param slotPerPage 每页的记录数
     * @return 位图字节数
     */
    private fun acquireBitMapLength(slotPerPage: Int): Int {
        return (slotPerPage + 7) shr 3
    }

    /**
     * 依据文件名删除文件
     * @param fileName 文件名
     * @return 是否删除成功
     */
    private fun removeFile(fileName: String): Boolean {
        return bufferManager.removeFile(fileName)
    }

    /**
     * 根据文件名打开文件，返回 FileHandler
     * @param fileName 文件名
     * @return 处理该文件的 [FileHandler]
     */
    private fun openFile(fileName: String): FileHandler {
        val file = bufferManager.openFile(fileName)
        return FileHandler(file, bufferManager)
    }

    /**
     * 打开 record 文件，返回 [FileHandler]
     */
    fun openRecord(databaseName: String, tableName: String): FileHandler =
        this.databases
            .getOrPut(databaseName) { mutableMapOf() }
            .getOrPut(tableName) { this.openFile(this.fileName(databaseName, tableName)) }

    fun renameRecord(databaseName: String, oldTableName: String, newTableName: String) {
        this.databases[databaseName]?.remove(oldTableName)?.let { fileHandler ->
            val file = fileHandler.file
            if (fileHandler.configChanged) {
                this.bufferManager.markDirty(file, 0)
            }
            this.bufferManager.renameFile(file, this.fileName(databaseName, newTableName))
        }
    }

    /**
     * 根据 FileHandler 关闭文件
     * @param fileHandler 待关闭文件的 Handler
     * @return 是否关闭成功
     */
    private fun closeFile(fileHandler: FileHandler): Boolean {
        val file = fileHandler.file
        if (fileHandler.configChanged) {
            bufferManager.markDirty(file, 0)
        } // 标记表头写回
        bufferManager.closeFile(file)
        return true
    }

    fun closeRecord(databaseName: String, tableName: String) {
        this.databases[databaseName]?.remove(tableName)?.let {
            this.closeFile(it)
        }
    }

    fun removeRecord(databaseName: String, tableName: String) {
        this.databases[databaseName]?.remove(tableName)?.let { fileHandler ->
            val file = fileHandler.file
            if (fileHandler.configChanged) {
                this.bufferManager.markDirty(file, 0)
            }
            this.bufferManager.removeFile(this.fileName(databaseName, tableName))
        }
    }

    fun closeDatabase(databaseName: String) {
        this.databases.remove(databaseName)?.values?.forEach { fileHandler ->
            this.closeFile(fileHandler)
        }
    }
}
