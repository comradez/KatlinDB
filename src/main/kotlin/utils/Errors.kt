package utils

class DatabaseAlreadyExistsError(databaseName: String) :
    Exception("Database `${databaseName}` already exists.")

class DatabaseNotExistsError(databaseName: String) :
    Exception("Database `${databaseName}` does not exist.")

class NoUsingDatabaseError : Exception("No database selected.")

class ColumnExistError(override val message: String?) : Exception(message)

class InternalError(override val message: String?) : Exception(message)
