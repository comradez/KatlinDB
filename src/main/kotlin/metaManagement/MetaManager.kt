package metaManagement

import pagedFile.FileManager

class MetaManager(
    _manager: FileManager,
    _homeDirectory: String = "./"
) : AutoCloseable {
    private val manager = _manager
    private val homeDirectory = _homeDirectory
    private val metaList = hashMapOf<String, MetaHandler>()

    override fun close() {
        metaList.values.forEach { it.close() }
    }

    fun openMeta(dbName: String) : MetaHandler {
        val handler = metaList[dbName]
        return handler ?: run {
            val newHandler = MetaHandler(dbName, homeDirectory)
            metaList[dbName] = newHandler
            newHandler
        }
    }

    fun closeMeta(dbName: String) {
        metaList.remove(dbName)?.close()
    }

    fun removeAll(dbName: String) {
        manager.removeFile("$dbName.meta")
    }

    fun shutdown() {
        metaList.keys.forEach { closeMeta(it) }
    }
}
