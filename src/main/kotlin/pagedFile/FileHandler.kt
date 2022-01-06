package pagedFile

import utils.BufferType

class FileHandler(private val bufferManager: BufferManager, _filename: String) : AutoCloseable {
    private val file = run {
        bufferManager.createFile(_filename)
        bufferManager.openFile(_filename)
    }

    fun readPage(pageId: Int) = this.bufferManager.readPage(this.file, pageId)

    fun updatePage(pageId: Int, buffer: BufferType) =
        this.bufferManager.updatePage(this.file, pageId, buffer)

    fun freshPage() = this.bufferManager.freshPage(this.file)

    override fun close() {
        this.bufferManager.closeFile(this.file)
    }
}
