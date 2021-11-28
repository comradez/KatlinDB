package pagedFile

import utils.MAX_FILE_NUMBER
import utils.PAGE_SHIFT
import utils.BufferType
import utils.PAGE_SIZE
import java.io.File
import java.io.RandomAccessFile

class FileManager {
    private val fileTable = arrayOfNulls<File>(MAX_FILE_NUMBER)

    /**
     * @brief 依据文件名创建文件
     * @param fileName 文件名
     * @return 文件是否创建成功
     */
    fun createFile(fileName: String): Boolean {
        val file = File(fileName)
        return file.createNewFile()
    }

    /**
     * @brief 依据文件名打开文件
     * @param fileName 文件名
     * @return 如文件存在且可读写，返回文件对象；否则返回null
     */
    fun openFile(fileName: String): File? {
        val fileId = fileTable.indexOf(null)
        if (fileId != -1) { // 还有剩余空间
            val file = File(fileName)
            if (file.exists() && file.isFile && file.canWrite() && file.canRead())  {
                fileTable[fileId] = file
                return file
            }
        }
        return null
    }

    /**
     * @brief 依据文件对象关闭文件
     * @param file 文件对象
     */
    fun closeFile(file: File) {
        val fileId = fileTable.indexOf(file)
        fileTable[fileId] = null
    }

    /**
     * @brief 依据文件名删除文件
     * @param fileName 文件名
     * @return 文件是否删除成功
     */
    fun removeFile(fileName: String): Boolean {
        val file = File(fileName)
        return file.delete()
    }

    /**
     * @brief 将 file 和 pageId 指定的文件页中 PAGE_SIZE 个字节读到 buffer 里并返回
     * @param file 文件对象
     * @param pageID 文件页号
     * @param buffer 缓冲区
     */
    fun readPage(file: File, pageID: Int, buffer: BufferType) {
        val offset = pageID shl PAGE_SHIFT // 一个 Page 的大小是 2 ^ PAGE_SHIFT = PAGE_SIZE
        val randomAccessFile = RandomAccessFile(file, "rw")
        if (randomAccessFile.length() < offset + PAGE_SIZE) {
            randomAccessFile.setLength((offset + PAGE_SIZE).toLong())
        } // 如果长度不足则扩展
        randomAccessFile.seek(offset.toLong())
        randomAccessFile.read(buffer, 0, PAGE_SIZE)
        randomAccessFile.close()
    }

    /**
     * @brief 将 buffer 中的 PAGE_SIZE 个字节写到 file 和 pageID 指定的文件页中
     * @param file 文件对象
     * @param pageID 文件页号
     * @param buffer 缓冲区
     */
    fun writePage(file: File, pageID: Int, buffer: BufferType) {
        val offset = pageID shl PAGE_SHIFT
        val randomAccessFile = RandomAccessFile(file, "rw")
        if (randomAccessFile.length() < offset + PAGE_SIZE) {
            randomAccessFile.setLength((offset + PAGE_SIZE).toLong())
        } // 如果长度不足则扩展
        randomAccessFile.seek(offset.toLong())
        randomAccessFile.write(buffer, 0, PAGE_SIZE)
        randomAccessFile.close()
    }

    /**
     * @brief 在 file 文件的末尾追加一个空白的页
     * @param file 文件对象
     * @return 新添加空白页的页号
     */
    fun freshPage(file: File) : Int {
        val randomAccessFile = RandomAccessFile(file, "rw")
        val offset = randomAccessFile.length()
        randomAccessFile.setLength(offset + PAGE_SIZE)
        return (offset shr PAGE_SHIFT).toInt()
    }
}
