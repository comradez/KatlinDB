import pf.BufferManager
import kotlin.test.Test
import kotlin.test.assertEquals

internal class BufferManagerTest {
    private val bufferManager = BufferManager()
    private val filename = "testFile"

    @Test
    fun writeAndReadFromBuffer() {
        bufferManager.createFile(filename)
        val testFile = bufferManager.openFile(filename)
        val bufferWrite = bufferManager.readPage(testFile, 1)
        for (i in bufferWrite.indices) {
            bufferWrite[i] = ('a' + i).code.toByte()
        }
        bufferManager.markDirty(testFile, 1)
        bufferManager.closeFile(testFile)

        val testFileAgain = bufferManager.openFile(filename)
        val bufferRead = bufferManager.readPage(testFileAgain, 1)
        for (i in bufferRead.indices) {
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
