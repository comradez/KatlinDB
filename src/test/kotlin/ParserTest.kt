import frontend.showResult
import kotlin.test.Test
import systemManagement.SystemManager
import java.nio.file.Paths

internal class ParserTest {
    private val manager = SystemManager(Paths.get("").toAbsolutePath().toString())
    private val createDatabase = "CREATE DATABASE DATASET;"
    private val useDatabase = "USE DATASET;"
    private val postOperation = "DROP DATABASE DATASET;"

    @Test
    fun executeNothing() {
        val results = manager.execute("")
        for (result in results) {
            showResult(result)
        }
    }

    private fun createAndUseDatabase() {
        val results = manager.execute(createDatabase + useDatabase)
        for (result in results) {
            showResult(result)
        }
    }

    @Test
    fun createTable() {
        createAndUseDatabase()
        val command = "CREATE TABLE PART (\n" +
                "    P_PARTKEY        INT,\n" +
                "    P_NAME           VARCHAR(55),\n" +
                "    P_MFGR           VARCHAR(25),\n" +
                "    P_BRAND          VARCHAR(10),\n" +
                "    P_TYPE           VARCHAR(25),\n" +
                "    P_SIZE           INT,\n" +
                "    P_CONTAINER      VARCHAR(10),\n" +
                "    P_RETAILPRICE    FLOAT,\n" +
                "    P_COMMENT        VARCHAR(23),\n" +
                "    PRIMARY KEY (P_PARTKEY)\n" +
                ");"
        val results = manager.execute(command)
        for (result in results) {
            showResult(result)
        }
        dropDatabase()
    }

    @Test
    fun alterTable() {
        createAndUseDatabase()
        val command = "ALTER TABLE SUPPLIER ADD CONSTRAINT TEST FOREIGN KEY (S_NATIONKEY) REFERENCES NATION(N_NATIONKEY);"
        val results = manager.execute(command)
        for (result in results) {
            showResult(result)
        }
        dropDatabase()
    }

    private fun dropDatabase() {
        manager.execute(postOperation)
    }
}