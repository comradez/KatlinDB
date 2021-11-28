package indexManagement

class IndexFile(
    _handler: IndexHandler,
    _rootId: Int
) {
    private val rootId = _rootId
        get
    private val handler = _handler
        get
    var dirty = false
}