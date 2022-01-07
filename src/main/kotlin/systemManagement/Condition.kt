package systemManagement

import recordManagement.CompareOp

typealias Predicate = (Any) -> Boolean

class Condition(val tableName: String?, val columnName: String, val predicate: Predicate) {
}

fun compareWith(compareOp: CompareOp, value: Any): Predicate {
    TODO()
}

fun existsIn(values: List<Any?>): Predicate {
    TODO()
}

fun hasPattern(pattern: String): Predicate {
    TODO()
}

fun isNull(): Predicate {
    TODO()
}

fun isNotNull(): Predicate {
    TODO()
}
