package parser

import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.ParseTree
import org.antlr.v4.runtime.tree.RuleNode
import org.antlr.v4.runtime.tree.TerminalNode
import systemManagement.SystemManager

class DatabaseVisitor(private val manager: SystemManager) : SQLBaseVisitor<QueryResult>() {

    var lastStartTime: Long? = null

    private fun measureTimeCost(): Long? {
        val currentTime = System.nanoTime()
        return if (lastStartTime != null) {
            val cost = currentTime - lastStartTime!!
            lastStartTime = currentTime
            cost
        } else {
            lastStartTime = currentTime
            null
        }
    }

    override fun visitProgram(ctx: SQLParser.ProgramContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitStatement(ctx: SQLParser.StatementContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitCreate_db(ctx: SQLParser.Create_dbContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitDrop_db(ctx: SQLParser.Drop_dbContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitShow_dbs(ctx: SQLParser.Show_dbsContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitUse_db(ctx: SQLParser.Use_dbContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitShow_tables(ctx: SQLParser.Show_tablesContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitShow_indexes(ctx: SQLParser.Show_indexesContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitLoad_data(ctx: SQLParser.Load_dataContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitDump_data(ctx: SQLParser.Dump_dataContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitCreate_table(ctx: SQLParser.Create_tableContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitDrop_table(ctx: SQLParser.Drop_tableContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitDescribe_table(ctx: SQLParser.Describe_tableContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitInsert_into_table(ctx: SQLParser.Insert_into_tableContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitDelete_from_table(ctx: SQLParser.Delete_from_tableContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitUpdate_table(ctx: SQLParser.Update_tableContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitSelect_table_(ctx: SQLParser.Select_table_Context?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitSelect_table(ctx: SQLParser.Select_tableContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitAlter_add_index(ctx: SQLParser.Alter_add_indexContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitAlter_drop_index(ctx: SQLParser.Alter_drop_indexContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitAlter_table_drop_pk(ctx: SQLParser.Alter_table_drop_pkContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitAlter_table_drop_foreign_key(ctx: SQLParser.Alter_table_drop_foreign_keyContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitAlter_table_add_pk(ctx: SQLParser.Alter_table_add_pkContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitAlter_table_add_foreign_key(ctx: SQLParser.Alter_table_add_foreign_keyContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitAlter_table_add_unique(ctx: SQLParser.Alter_table_add_uniqueContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitField_list(ctx: SQLParser.Field_listContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitNormal_field(ctx: SQLParser.Normal_fieldContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitPrimary_key_field(ctx: SQLParser.Primary_key_fieldContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitForeign_key_field(ctx: SQLParser.Foreign_key_fieldContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitType_(ctx: SQLParser.Type_Context?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitValue_lists(ctx: SQLParser.Value_listsContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitValue_list(ctx: SQLParser.Value_listContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitValue(ctx: SQLParser.ValueContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitWhere_and_clause(ctx: SQLParser.Where_and_clauseContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitWhere_operator_expression(ctx: SQLParser.Where_operator_expressionContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitWhere_operator_select(ctx: SQLParser.Where_operator_selectContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitWhere_null(ctx: SQLParser.Where_nullContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitWhere_in_list(ctx: SQLParser.Where_in_listContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitWhere_in_select(ctx: SQLParser.Where_in_selectContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitWhere_like_string(ctx: SQLParser.Where_like_stringContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitColumn(ctx: SQLParser.ColumnContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitExpression(ctx: SQLParser.ExpressionContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitSet_clause(ctx: SQLParser.Set_clauseContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitSelectors(ctx: SQLParser.SelectorsContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitSelector(ctx: SQLParser.SelectorContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitIdentifiers(ctx: SQLParser.IdentifiersContext?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitOperator_(ctx: SQLParser.Operator_Context?): QueryResult {
        TODO("Not yet implemented")
    }

    override fun visitAggregator(ctx: SQLParser.AggregatorContext?): QueryResult {
        TODO("Not yet implemented")
    }
}
