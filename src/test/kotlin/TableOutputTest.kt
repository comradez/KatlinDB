import frontend.showResult
import org.junit.jupiter.api.Test
import parser.EmptyResult
import parser.ErrorResult
import parser.SuccessResult

internal class TableOutputTest {
    private val successResult = SuccessResult(
        listOf("Column1", "Column2", "Column3"),
        listOf(
            listOf("a", "b", "c"),
            listOf("d", "e", "f")
        )
    )
    private val emptyResult = EmptyResult()
    private val errorResult = ErrorResult("No using database")
    init {
        successResult.timeCost = 1000000
        emptyResult.timeCost = 2000000
        errorResult.timeCost = 3000000
    }

    @Test
    fun showResultsTest() {
        showResult(successResult)
        showResult(emptyResult)
        showResult(errorResult)
    }
}