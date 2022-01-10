import frontend.enterCommandLineEnvironment
import frontend.executeSqlFile
import org.apache.commons.cli.*
import java.io.File
import java.nio.file.Paths

fun main(args: Array<String>) {
    val execOption = Option.builder("execute")
        .argName("file")
        .hasArg()
        .desc("execute the given sql file")
        .build()
    val cliOption = Option("cli", "use the command line interface")
    val helpOption = Option("help", "print the help message")
    val workDirectoryOption = Option.builder("workdir")
        .argName("workdir")
        .hasArg()
        .desc("use the given directory as the working directory")
        .build()
    val options = Options()
    options.addOption(execOption)
    options.addOption(cliOption)
    options.addOption(workDirectoryOption)
    options.addOption(helpOption)
    val parser = DefaultParser()
    try {
        val commandLine = parser.parse(options, args)
        val workDirectory = if (commandLine.hasOption("workdir")) {
            commandLine.getOptionValue("workdir")
        } else {
            println("Info: working directory not specified, using current directory")
            Paths.get("").toAbsolutePath().toString()
        }
        if (commandLine.hasOption("execute")) {
            executeSqlFile(commandLine.getOptionValue("execute"), workDirectory)
        } else if (commandLine.hasOption("cli")) {
            enterCommandLineEnvironment(workDirectory)
        } else {
            val formatter = HelpFormatter()
            formatter.printHelp("KatlinDB", options)
        }
    } catch (e: ParseException) {
        println("ParseError: ${e.message}")
    }
}
