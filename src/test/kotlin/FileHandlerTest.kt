import com.github.michaelbull.result.Ok
import com.github.michaelbull.result.get
import recordManagement.*
import kotlin.test.Test

internal class FileHandlerTest {
    private val recordHandler = RecordHandler()
    private val fileName =  "testFile2"

    @Test
    fun someTest() {
        recordHandler.removeFile(fileName)
        recordHandler.createFile(fileName, 8)
        val fileHandler = checkNotNull(recordHandler.openFile(fileName).get())
        for (i in 0 .. 25) {
            val buffer = ByteArray(8)
            for (j in buffer.indices) {
                buffer[j] = if (i % 2 == 0) {
                    ('a' + j).code.toByte()
                } else {
                    ('A' + j).code.toByte()
                }
            }
            assert(fileHandler.insertRecord(buffer) is Ok)
        }
        val rid = RID(1, 4)
        assert(fileHandler.deleteRecord(rid) is Ok)
        assert(fileHandler.insertRecord("!@#$%^&*".toByteArray()) is Ok)
        assert(fileHandler.insertRecord("!@#$%^&*".toByteArray()) is Ok)
        val buffer = checkNotNull(fileHandler.getRecord(RID(1, 4)).get()).data
        assert(buffer.decodeToString() == "!@#$%^&*")
        recordHandler.closeFile(fileHandler)
    }

    @Test
    fun fileScanTest() {
        recordHandler.removeFile(fileName)
        recordHandler.createFile(fileName, 8)
        val fileHandler = checkNotNull(recordHandler.openFile(fileName).get())
        for (i in 0 .. 25) {
            val buffer = ByteArray(8)
            if (i % 2 == 0) {
                buffer[0] = 0x78
                buffer[1] = 0x56
                buffer[2] = 0x34
                buffer[3] = 0x12
                buffer[4] = 0x12
                buffer[5] = 0x34
                buffer[6] = 0x56
                buffer[7] = 0x78
            } else {
                buffer[0] = 0x12
                buffer[1] = 0x34
                buffer[2] = 0x56
                buffer[3] = 0x78
                buffer[4] = 0x78
                buffer[5] = 0x56
                buffer[6] = 0x34
                buffer[7] = 0x12
            }
            assert(fileHandler.insertRecord(buffer) is Ok)
        }
        val value = 0x00345678
        val fileScan = FileScan()
        val result = fileScan.openScan(fileHandler, AttributeType.INT, 4, 0, CompareOp.LT_OP, value)
        result.toList().forEach { println("RID: ${it.rid.first}, ${it.rid.second}") }
        recordHandler.closeFile(fileHandler)
    }
}