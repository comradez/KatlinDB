package utils

class DatabaseAlreadyExistsError(databaseName: String) :
    Exception("Database `${databaseName}` already exists.")

class DatabaseNotExistsError(databaseName: String) :
    Exception("Database `${databaseName}` does not exist.")

class NoUsingDatabaseError : Exception("No database selected.")

class IndexAlreadyExistsError(indexName: String) :
    Exception("Index `${indexName}` already exists.")

class IndexNotExistsError(indexName: String) :
    Exception("Index `${indexName}` does not exist.")

class TableAlreadyExistsError(tableName: String) :
    Exception("Table `${tableName}` already exists.")

class TableNotExistsError(tableName: String) :
    Exception("Table `${tableName}` does not exist.")

class ColumnAlreadyIndexedError(tableName: String, columnName: String) :
    Exception("Column `${columnName}` in table `${tableName}` already has an index.")

class ColumnAlreadyExistsError(tableName: String, columnName: String) :
    Exception("Column `${columnName}` in table `${tableName}` already exists.")

class ColumnNotExistsError(tableName: String, columnName: String) :
    Exception("Column `${columnName}` in table `${tableName}` does not exist.")

class NotForeignKeyColumnError(tableName: String, columnName: String) :
    Exception("Column `${columnName}` in table `${tableName}` is not a foreign key.")

class TypeMismatchError(value: Any?) :
    Exception("Type of `${value}` mismatched.")

class ConstraintViolationError(override val message: String) : Exception(message)

class IllFormSelectError(override val message: String) : Exception(message)

class BadColumnIdentifier(override val message: String) : Exception(message)

class FileAlreadyExistsError(filename: String) :
    Exception("Target file `$filename` already exists.")

class FileNotExistError(filename: String) :
    Exception("Target file `$filename` does not exist or is not a file.")

class InternalError(override val message: String) : Exception(message)
