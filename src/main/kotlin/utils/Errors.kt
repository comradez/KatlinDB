package utils

import java.lang.Exception

class ColumnExistError(override val message: String?) : Exception(message)

class InternalError(override val message: String?) : Exception(message)