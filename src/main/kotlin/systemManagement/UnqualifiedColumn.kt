package systemManagement

data class UnqualifiedColumn(var tableName: String?, val columnName: String) {
    override fun toString(): String =
        if (this.tableName == null) {
            this.columnName
        } else {
            "${this.tableName}.${this.columnName}"
        }
}
