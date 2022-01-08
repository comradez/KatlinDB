import pagedFile.BufferManager
import pagedFile.FileManager
import kotlin.test.Test
import kotlin.test.assertEquals

internal class BufferManagerTest {
    private val bufferManager = BufferManager(FileManager())
    private val filename = "testFile"

    @Test
    fun writeAndReadFromBuffer() {
        bufferManager.createFile(filename)
        val testFile = bufferManager.openFile(filename)
        val bufferWrite = bufferManager.readPage(testFile, 1)
        for (i in 0 until 16) {
            bufferWrite[i] = ('a' + i).code.toByte()
        }
        bufferManager.markDirty(testFile, 1)
        bufferManager.closeFile(testFile)

        val testFileAgain = bufferManager.openFile(filename)
        val bufferRead = bufferManager.readPage(testFileAgain, 1)
        for (i in 0 until 16) {
            assertEquals(
                'a' + i,
                bufferRead[i].toInt().toChar(),
                "index $i not match, expected ${'a' + i}, found ${bufferRead[i].toInt().toChar()}"
            )
        }
        bufferManager.closeFile(testFileAgain)
        bufferManager.removeFile(filename)
    }
}
