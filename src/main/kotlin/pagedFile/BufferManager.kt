package pagedFile

import utils.BufferType
import utils.PAGE_NUMBER
import utils.PAGE_SIZE
import java.io.File
import java.io.FileNotFoundException

class BufferManager(private val fileManager: FileManager) : AutoCloseable {
    private val findReplace = FindReplace(PAGE_NUMBER)

    private val pageBuffers = arrayOfNulls<BufferType>(PAGE_NUMBER) // 页缓冲区
    private val pageDirty = BooleanArray(PAGE_NUMBER) {
        false
    } // 标记页面是否脏（用于写回）
    private val indexToFilePage = arrayOfNulls<Pair<File, Int>>(PAGE_NUMBER)

    // 页缓冲区下标 -> (文件, 页号)
    private val filePageToIndex: HashMap<Pair<File, Int>, Int> = HashMap()

    // (文件, 页号) -> 页缓冲区下标
    private val fileCachePages = HashMap<File, HashSet<Int>>()
    // 记录一个文件的哪些页在缓存中

    override fun close() {
        this.fileCachePages.forEach { (file, pages) ->
            pages.forEach { this.writeBack(it) }
            this.fileManager.closeFile(file)
        }
    }

    /**
     * @brief 根据文件名创建文件
     * @param fileName 文件名
     * @return 创建是否成功
     */
    fun createFile(fileName: String): Boolean {
        return fileManager.createFile(fileName)
    }

    /**
     * @brief 根据文件名打开文件
     * @param fileName 文件名
     * @return 打开成功则返回文件对象，失败返回null
     */
    fun openFile(fileName: String): File {
        val file = fileManager.openFile(fileName)
        if (file != null) {
            fileCachePages[file] = HashSet()
            return file
        }
        throw FileNotFoundException("file $fileName not found!")
    }

    /**
     * @brief 根据文件对象关闭文件
     * @param file 文件对象
     * @return 关闭是否成功
     */
    fun closeFile(file: File) {
        fileCachePages.remove(file)?.forEach {
            writeBack(it)
        } // 写回对应这个文件的所有已缓存的页面
        fileManager.closeFile(file)
    }

    fun renameFile(file: File, newName: String): Boolean {
        fileCachePages.remove(file)?.forEach { writeBack(it) }
        return this.fileManager.renameFile(file, newName)
    }

    /**
     * @brief 根据文件名删除文件
     * @param fileName 文件名
     * @return 删除是否成功
     */
    fun removeFile(fileName: String): Boolean {
        return fileManager.removeFile(fileName)
    }

    /**
     * @brief 根据文件对象和页码，获取一个页的引用
     * @param file 文件对象
     * @param pageId 页码数
     * @return 对应页缓冲区引用
     * @remark 如果在缓存中则直接拿来，如果不在缓存中则需要LRU分配一个缓冲区空间然后放入缓冲区
     */
    fun readPage(file: File, pageId: Int): BufferType {
        val filePagePair = Pair(file, pageId)
        val position = filePageToIndex.getOrDefault(filePagePair, -1)
        if (position != -1) { // 缓存中存在这个页
            return checkNotNull(pageBuffers[position])
        } // 否则缓存中不存在这个页，去底层读取
        val index = findReplace.getIndex() // 获取一个新的可用的 index
        if (indexToFilePage[index] != null) { // 写回原来此处的页
            writeBack(index)
        }
        fileCachePages[file]?.add(index)
        filePageToIndex[filePagePair] = index
        indexToFilePage[index] = filePagePair
        val buffer = ByteArray(PAGE_SIZE)
//        val buffer: ByteBuffer = BufferType.allocate(PAGE_SIZE)
        fileManager.readPage(file, pageId, buffer)
        pageBuffers[index] = buffer
        return checkNotNull(pageBuffers[index])
    }

    /**
     * @brief 在关闭前写回页缓冲区中的一页（如需要）并清除它在缓存中的记录
     * @param index 页缓冲区中的下标
     */
    private fun writeBack(index: Int) {
        val (file, pageId) = checkNotNull(indexToFilePage[index])
        if (pageDirty[index]) {
            fileManager.writePage(file, pageId, checkNotNull(pageBuffers[index]))
            pageDirty[index] = false
        }
        findReplace.free(index)
        fileCachePages[file]?.remove(index)
        filePageToIndex.remove(Pair(file, pageId))
        indexToFilePage[index] = null
    }

    /**
     * @brief 根据文件对象和页码，修改一个页的内容
     * @param file 文件对象
     * @param pageId 页码数
     * @param newBuffer 新的页面内容
     * @remark 如果在缓存中则直接拿来，如果不在缓存中则需要LRU分配一个缓冲区空间然后放入缓冲区
     */
    fun updatePage(file: File, pageId: Int, newBuffer: ByteArray) {
        val filePagePair = Pair(file, pageId)
        val position = filePageToIndex.getOrDefault(filePagePair, -1)
        if (position != -1) { // 缓存中存在这个页
            pageBuffers[position] = newBuffer
        } else { // 否则缓存中不存在这个页
            val index = findReplace.getIndex() // 获取一个新的可用的 index
            if (indexToFilePage[index] != null) { // 写回原来此处的页
                writeBack(index)
            }
            fileCachePages[file]?.add(index)
            filePageToIndex[filePagePair] = index
            indexToFilePage[index] = filePagePair
            pageBuffers[index] = newBuffer
        }
        markDirty(file, pageId)
    }

    /**
     * @brief 根据文件对象和页码，将一个页置为脏的（需要写回）
     * @param file 文件对象
     * @param pageId 页码数
     */
    fun markDirty(file: File, pageId: Int) {
        val filePagePair = Pair(file, pageId)
        val index = filePageToIndex.getOrDefault(filePagePair, -1)
        if (index != -1) {
            pageDirty[index] = true
        }
    }

    fun forceWriteBack(file: File, pageId: Int) {
        val filePagePair = Pair(file, pageId)
        val index = filePageToIndex.get(filePagePair)
        if (index != null) {
            writeBack(index)
        }
    }

    /**
     * @brief 在 file 文件的末尾追加一个空白的页
     * @param file 文件对象
     * @return 新添加空白页的页号
     */
    fun freshPage(file: File): Int {
        return fileManager.freshPage(file)
    }
}
