package indexManagement

import pagedFile.BufferManager
import pagedFile.FileManager
import java.io.File

class IndexHandler(
    _bufferManager: BufferManager,
    _databaseName: String
) {
    private val bufferManager = _bufferManager
    private val databaseName = _databaseName
    private val indexFile: File
    private var dirty = false

    init {
        val indexFileName = "${databaseName}.index"
        bufferManager.createFile(indexFileName)
        indexFile = bufferManager.openFile(indexFileName)
    }

    fun readPage(pageId: Int) : ByteArray = bufferManager.readPage(indexFile, pageId)

    fun updatePage(pageId: Int, byteArray: ByteArray) {
        bufferManager.updatePage(indexFile, pageId, byteArray)
        dirty = true
    }
    fun freshPage() : Int = bufferManager.freshPage(indexFile)
}