package frontend

import parser.EmptyResult
import parser.ErrorResult
import parser.QueryResult
import parser.SuccessResult
import systemManagement.SystemManager
import java.io.File
import java.util.*

fun enterCommandLineEnvironment(workDir: String) {
    val manager = SystemManager(workDir)
    val scanner = Scanner(System.`in`)
    var command: String
    var nextLine: String
    while (true) {
        command = ""
        println("[DataBase: ${manager.selectedDatabase ?: "None"}]")
        print(">>> ")
        do {
            try {
                nextLine = scanner.nextLine()
                command = "$command $nextLine"
                if (nextLine.isEmpty() || nextLine[nextLine.length - 1] != ';') {
                    print("... ")
                }
            } catch (_: NoSuchElementException) {
                command = "exit;"
                break
            }
        } while (nextLine.isEmpty() || nextLine[nextLine.length - 1] != ';')
        if (command.trim(';').trim().lowercase() == "exit") {
            break
        }
        val results = manager.execute(command)
        assert(results.size == 1)
        showResult(results[0])
    }
}

fun executeSqlFile(filename: String, workDir: String) {
    val file = File(filename)
    if (!file.exists()) {
        println("FileError: File $file doesn't exist.")
    } else if (!file.isFile) {
        println("FileError: $file is not a file.")
    }
    val manager = SystemManager(workDir)
    val results = manager.execute(file.readText())
    for (result in results) {
        showResult(result)
    }
}

fun showResult(result: QueryResult) {
    when (result) {
        is SuccessResult -> {
            print(result.outputTable() ?: "(No output)\n")
            println("${result.timeCost?.div(1000)} ms spent.")
        }
        is EmptyResult -> {
            println("(No output)\n" +
                    "${result.timeCost?.div(1000)} ms spent.")
        }
        is ErrorResult -> {
            println("Operation failed with error message ${result.errorMessage}\n" +
                    "${result.timeCost} ms spent.")
        }
    }
}