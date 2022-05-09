import adoApi.Field
import com.google.gson.JsonElement
import com.google.gson.JsonPrimitive
import mu.KotlinLogging
import java.sql.*

public class AdoJdbcDatabaseMetaData(
    private val connection: AdoJdbcConnection
) : DatabaseMetaData {
    private val logger = KotlinLogging.logger {}
    private val workItemsService = this.connection.workItemsService

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        logger.trace { "unwrap $iface " }
        throw SQLFeatureNotSupportedException("not implemented")
    }


    private fun emptyResultSet(): ResultSet {
        val rows = listOf<Map<String, JsonElement>>()
        val cols = listOf<ColumnInfo>()
        return JsonResultSet(WiqlStatement(this.connection), rows, cols)
    }


    override fun isWrapperFor(iface: Class<*>?): Boolean {
        logger.trace { "isWrapperFor $iface " }
        return false
    }

    override fun allProceduresAreCallable(): Boolean {
        logger.trace { "allProceduresAreCallable  " }
        return true
    }

    override fun allTablesAreSelectable(): Boolean {
        logger.trace { "allTablesAreSelectable  " }
        return true
    }

    override fun getURL(): String? {
        logger.trace { "getURL  " }
        return null
    }

    override fun getUserName(): String? {
        logger.trace { "getUserName  " }
        return null
    }

    override fun isReadOnly(): Boolean {
        logger.trace { "isReadOnly  " }
        return true
    }

    override fun nullsAreSortedHigh(): Boolean {
        logger.trace { "nullsAreSortedHigh  " }
        return false
    }

    override fun nullsAreSortedLow(): Boolean {
        logger.trace { "nullsAreSortedLow  " }
        return false
    }

    override fun nullsAreSortedAtStart(): Boolean {
        logger.trace { "nullsAreSortedAtStart  " }
        return false
    }

    override fun nullsAreSortedAtEnd(): Boolean {
        logger.trace { "nullsAreSortedAtEnd  " }
        return false
    }

    override fun getDatabaseProductName(): String {
        logger.trace { "getDatabaseProductName  " }
        return "Azure DevOps"
    }

    override fun getDatabaseProductVersion(): String {
        logger.trace { "getDatabaseProductVersion  " }
        return "1.0.0"
    }

    override fun getDriverName(): String {
        logger.trace { "getDriverName  " }
        return Constants.DRIVER_NAME;
    }

    override fun getDriverVersion(): String {
        logger.trace { "getDriverVersion  " }
        return Constants.DRIVER_VERSION;
    }

    override fun getDriverMajorVersion(): Int {
        logger.trace { "getDriverMajorVersion  " }
        return Constants.DRIVER_VERSION_MAJOR
    }

    override fun getDriverMinorVersion(): Int {
        logger.trace { "getDriverMinorVersion  " }
        return Constants.DRIVER_VERSION_MINOR
    }

    override fun usesLocalFiles(): Boolean {
        logger.trace { "usesLocalFiles  " }
        return false
    }

    override fun usesLocalFilePerTable(): Boolean {
        logger.trace { "usesLocalFilePerTable  " }
        return false
    }

    override fun supportsMixedCaseIdentifiers(): Boolean {
        logger.trace { "supportsMixedCaseIdentifiers  " }
        return false
    }

    override fun storesUpperCaseIdentifiers(): Boolean {
        logger.trace { "storesUpperCaseIdentifiers  " }
        return false
    }

    override fun storesLowerCaseIdentifiers(): Boolean {
        logger.trace { "storesLowerCaseIdentifiers  " }
        return false
    }

    override fun storesMixedCaseIdentifiers(): Boolean {
        logger.trace { "storesMixedCaseIdentifiers  " }
        return false
    }

    override fun supportsMixedCaseQuotedIdentifiers(): Boolean {
        logger.trace { "supportsMixedCaseQuotedIdentifiers  " }
        return false
    }

    override fun storesUpperCaseQuotedIdentifiers(): Boolean {
        logger.trace { "storesUpperCaseQuotedIdentifiers  " }
        return false
    }

    override fun storesLowerCaseQuotedIdentifiers(): Boolean {
        logger.trace { "storesLowerCaseQuotedIdentifiers  " }
        return false
    }

    override fun storesMixedCaseQuotedIdentifiers(): Boolean {
        logger.trace { "storesMixedCaseQuotedIdentifiers  " }
        return false
    }

    override fun getIdentifierQuoteString(): String? {
        logger.trace { "getIdentifierQuoteString  " }
        return null
    }

    override fun getSQLKeywords(): String {
        logger.trace { "getSQLKeywords  " }
        return "ASOF"
    }

    override fun getNumericFunctions(): String? {
        logger.trace { "getNumericFunctions  " }
        return null
    }

    override fun getStringFunctions(): String? {
        logger.trace { "getStringFunctions  " }
        return null
    }

    override fun getSystemFunctions(): String? {
        logger.trace { "getSystemFunctions  " }
        return null
    }

    override fun getTimeDateFunctions(): String? {
        logger.trace { "getTimeDateFunctions  " }
        return null
    }

    override fun getSearchStringEscape(): String? {
        logger.trace { "getSearchStringEscape  " }
        return null
    }

    override fun getExtraNameCharacters(): String? {
        logger.trace { "getExtraNameCharacters  " }
        return null

    }

    override fun supportsAlterTableWithAddColumn(): Boolean {
        logger.trace { "supportsAlterTableWithAddColumn  " }
        return false
    }

    override fun supportsAlterTableWithDropColumn(): Boolean {
        logger.trace { "supportsAlterTableWithDropColumn  " }
        return false
    }

    override fun supportsColumnAliasing(): Boolean {
        logger.trace { "supportsColumnAliasing  " }
        return false
    }

    override fun nullPlusNonNullIsNull(): Boolean {
        logger.trace { "nullPlusNonNullIsNull  " }
        return false
    }

    override fun supportsConvert(): Boolean {
        logger.trace { "supportsConvert  " }
        return false
    }

    override fun supportsConvert(fromType: Int, toType: Int): Boolean {
        logger.trace { "supportsConvert $fromType $toType " }
        return false
    }

    override fun supportsTableCorrelationNames(): Boolean {
        logger.trace { "supportsTableCorrelationNames  " }
        return false
    }

    override fun supportsDifferentTableCorrelationNames(): Boolean {
        logger.trace { "supportsDifferentTableCorrelationNames  " }
        return false
    }

    override fun supportsExpressionsInOrderBy(): Boolean {
        logger.trace { "supportsExpressionsInOrderBy  " }
        return false
    }

    override fun supportsOrderByUnrelated(): Boolean {
        logger.trace { "supportsOrderByUnrelated  " }
        return false
    }

    override fun supportsGroupBy(): Boolean {
        logger.trace { "supportsGroupBy  " }
        return false
    }

    override fun supportsGroupByUnrelated(): Boolean {
        logger.trace { "supportsGroupByUnrelated  " }
        return false
    }

    override fun supportsGroupByBeyondSelect(): Boolean {
        logger.trace { "supportsGroupByBeyondSelect  " }
        return false
    }

    override fun supportsLikeEscapeClause(): Boolean {
        logger.trace { "supportsLikeEscapeClause  " }
        return false
    }

    override fun supportsMultipleResultSets(): Boolean {
        logger.trace { "supportsMultipleResultSets  " }
        return false
    }

    override fun supportsMultipleTransactions(): Boolean {
        logger.trace { "supportsMultipleTransactions  " }
        return false
    }

    override fun supportsNonNullableColumns(): Boolean {
        logger.trace { "supportsNonNullableColumns  " }
        return false
    }

    override fun supportsMinimumSQLGrammar(): Boolean {
        logger.trace { "supportsMinimumSQLGrammar  " }
        return false
    }

    override fun supportsCoreSQLGrammar(): Boolean {
        logger.trace { "supportsCoreSQLGrammar  " }
        return false
    }

    override fun supportsExtendedSQLGrammar(): Boolean {
        logger.trace { "supportsExtendedSQLGrammar  " }
        return false
    }

    override fun supportsANSI92EntryLevelSQL(): Boolean {
        logger.trace { "supportsANSI92EntryLevelSQL  " }
        return false
    }

    override fun supportsANSI92IntermediateSQL(): Boolean {
        logger.trace { "supportsANSI92IntermediateSQL  " }
        return false
    }

    override fun supportsANSI92FullSQL(): Boolean {
        logger.trace { "supportsANSI92FullSQL  " }
        return false
    }

    override fun supportsIntegrityEnhancementFacility(): Boolean {
        logger.trace { "supportsIntegrityEnhancementFacility  " }
        return false
    }

    override fun supportsOuterJoins(): Boolean {
        logger.trace { "supportsOuterJoins  " }
        return false
    }

    override fun supportsFullOuterJoins(): Boolean {
        logger.trace { "supportsFullOuterJoins  " }
        return false
    }

    override fun supportsLimitedOuterJoins(): Boolean {
        logger.trace { "supportsLimitedOuterJoins  " }
        return false
    }

    override fun getSchemaTerm(): String {
        logger.trace { "getSchemaTerm  " }
        return "SCHEMA"
    }

    override fun getProcedureTerm(): String {
        logger.trace { "getProcedureTerm  " }
        return "PROCEDURE"
    }

    override fun getCatalogTerm(): String {
        logger.trace { "getCatalogTerm  " }
        return "CATALOG"
    }

    override fun isCatalogAtStart(): Boolean {
        logger.trace { "isCatalogAtStart  " }
        return false
    }

    override fun getCatalogSeparator(): String {
        logger.trace { "getCatalogSeparator  " }
        return "."
    }

    override fun supportsSchemasInDataManipulation(): Boolean {
        logger.trace { "supportsSchemasInDataManipulation  " }
        return false
    }

    override fun supportsSchemasInProcedureCalls(): Boolean {
        logger.trace { "supportsSchemasInProcedureCalls  " }
        return false
    }

    override fun supportsSchemasInTableDefinitions(): Boolean {
        logger.trace { "supportsSchemasInTableDefinitions  " }
        return false
    }

    override fun supportsSchemasInIndexDefinitions(): Boolean {
        logger.trace { "supportsSchemasInIndexDefinitions  " }
        return false
    }

    override fun supportsSchemasInPrivilegeDefinitions(): Boolean {
        logger.trace { "supportsSchemasInPrivilegeDefinitions  " }
        return false
    }

    override fun supportsCatalogsInDataManipulation(): Boolean {
        logger.trace { "supportsCatalogsInDataManipulation  " }
        return false
    }

    override fun supportsCatalogsInProcedureCalls(): Boolean {
        logger.trace { "supportsCatalogsInProcedureCalls  " }
        return false
    }

    override fun supportsCatalogsInTableDefinitions(): Boolean {
        logger.trace { "supportsCatalogsInTableDefinitions  " }
        return false
    }

    override fun supportsCatalogsInIndexDefinitions(): Boolean {
        logger.trace { "supportsCatalogsInIndexDefinitions  " }
        return false
    }

    override fun supportsCatalogsInPrivilegeDefinitions(): Boolean {
        logger.trace { "supportsCatalogsInPrivilegeDefinitions  " }
        return false
    }

    override fun supportsPositionedDelete(): Boolean {
        logger.trace { "supportsPositionedDelete  " }
        return false
    }

    override fun supportsPositionedUpdate(): Boolean {
        logger.trace { "supportsPositionedUpdate  " }
        return false
    }

    override fun supportsSelectForUpdate(): Boolean {
        logger.trace { "supportsSelectForUpdate  " }
        return false
    }

    override fun supportsStoredProcedures(): Boolean {
        logger.trace { "supportsStoredProcedures  " }
        return false
    }

    override fun supportsSubqueriesInComparisons(): Boolean {
        logger.trace { "supportsSubqueriesInComparisons  " }
        return false
    }

    override fun supportsSubqueriesInExists(): Boolean {
        logger.trace { "supportsSubqueriesInExists  " }
        return false
    }

    override fun supportsSubqueriesInIns(): Boolean {
        logger.trace { "supportsSubqueriesInIns  " }
        return false
    }

    override fun supportsSubqueriesInQuantifieds(): Boolean {
        logger.trace { "supportsSubqueriesInQuantifieds  " }
        return false
    }

    override fun supportsCorrelatedSubqueries(): Boolean {
        logger.trace { "supportsCorrelatedSubqueries  " }
        return false
    }

    override fun supportsUnion(): Boolean {
        logger.trace { "supportsUnion  " }
        return false
    }

    override fun supportsUnionAll(): Boolean {
        logger.trace { "supportsUnionAll  " }
        return false
    }

    override fun supportsOpenCursorsAcrossCommit(): Boolean {
        logger.trace { "supportsOpenCursorsAcrossCommit  " }
        return false
    }

    override fun supportsOpenCursorsAcrossRollback(): Boolean {
        logger.trace { "supportsOpenCursorsAcrossRollback  " }
        return false
    }

    override fun supportsOpenStatementsAcrossCommit(): Boolean {
        logger.trace { "supportsOpenStatementsAcrossCommit  " }
        return false
    }

    override fun supportsOpenStatementsAcrossRollback(): Boolean {
        logger.trace { "supportsOpenStatementsAcrossRollback  " }
        return false
    }

    override fun getMaxBinaryLiteralLength(): Int {
        logger.trace { "getMaxBinaryLiteralLength  " }
        return Integer.MAX_VALUE
    }

    override fun getMaxCharLiteralLength(): Int {
        logger.trace { "getMaxCharLiteralLength  " }
        return Integer.MAX_VALUE
    }

    override fun getMaxColumnNameLength(): Int {
        logger.trace { "getMaxColumnNameLength  " }
        return 300
    }

    override fun getMaxColumnsInGroupBy(): Int {
        logger.trace { "getMaxColumnsInGroupBy  " }
        return 300
    }

    override fun getMaxColumnsInIndex(): Int {
        logger.trace { "getMaxColumnsInIndex  " }
        return 300
    }

    override fun getMaxColumnsInOrderBy(): Int {
        logger.trace { "getMaxColumnsInOrderBy  " }
        return 300
    }

    override fun getMaxColumnsInSelect(): Int {
        logger.trace { "getMaxColumnsInSelect  " }
        return Integer.MAX_VALUE
    }

    override fun getMaxColumnsInTable(): Int {
        logger.trace { "getMaxColumnsInTable  " }
        return Integer.MAX_VALUE
    }

    override fun getMaxConnections(): Int {
        logger.trace { "getMaxConnections  " }
        return 0
    }

    override fun getMaxCursorNameLength(): Int {
        logger.trace { "getMaxCursorNameLength  " }
        return 100
    }

    override fun getMaxIndexLength(): Int {
        logger.trace { "getMaxIndexLength  " }
        return 300
    }

    override fun getMaxSchemaNameLength(): Int {
        logger.trace { "getMaxSchemaNameLength  " }
        return 300
    }

    override fun getMaxProcedureNameLength(): Int {
        logger.trace { "getMaxProcedureNameLength  " }
        return 300
    }

    override fun getMaxCatalogNameLength(): Int {
        logger.trace { "getMaxCatalogNameLength  " }
        return 300
    }

    override fun getMaxRowSize(): Int {
        logger.trace { "getMaxRowSize  " }
        return Integer.MAX_VALUE
    }

    override fun doesMaxRowSizeIncludeBlobs(): Boolean {
        logger.trace { "doesMaxRowSizeIncludeBlobs  " }
        return false
    }

    override fun getMaxStatementLength(): Int {
        logger.trace { "getMaxStatementLength  " }
        return Integer.MAX_VALUE
    }

    override fun getMaxStatements(): Int {
        logger.trace { "getMaxStatements  " }
        return 300
    }

    override fun getMaxTableNameLength(): Int {
        logger.trace { "getMaxTableNameLength  " }
        return 300
    }

    override fun getMaxTablesInSelect(): Int {
        logger.trace { "getMaxTablesInSelect  " }
        return Integer.MAX_VALUE
    }

    override fun getMaxUserNameLength(): Int {
        logger.trace { "getMaxUserNameLength  " }
        return 300
    }

    override fun getDefaultTransactionIsolation(): Int {
        logger.trace { "getDefaultTransactionIsolation  " }
        return 0
    }

    override fun supportsTransactions(): Boolean {
        logger.trace { "supportsTransactions  " }
        return false
    }

    override fun supportsTransactionIsolationLevel(level: Int): Boolean {
        logger.trace { "supportsTransactionIsolationLevel $level " }
        return false
    }

    override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean {
        logger.trace { "supportsDataDefinitionAndDataManipulationTransactions  " }
        return false
    }

    override fun supportsDataManipulationTransactionsOnly(): Boolean {
        logger.trace { "supportsDataManipulationTransactionsOnly  " }
        return false
    }

    override fun dataDefinitionCausesTransactionCommit(): Boolean {
        logger.trace { "dataDefinitionCausesTransactionCommit  " }
        return false
    }

    override fun dataDefinitionIgnoredInTransactions(): Boolean {
        logger.trace { "dataDefinitionIgnoredInTransactions  " }
        return false
    }

    override fun getProcedures(catalog: String?, schemaPattern: String?, procedureNamePattern: String?): ResultSet {
        logger.trace { "getProcedures $catalog $schemaPattern $procedureNamePattern " }
        return emptyResultSet()
    }

    override fun getProcedureColumns(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        val rows = listOf<Map<String, JsonElement>>()
        val cols = listOf<ColumnInfo>()
        return JsonResultSet(WiqlStatement(this.connection), rows, cols)
    }

    override fun getTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        types: Array<out String>?
    ): ResultSet {
        val tableNames = Constants.JdbcTableColumns;
        val rows = listOf(
            mapOf(
                tableNames.CATALOG to null,
                tableNames.SCHEMA to null,
                tableNames.NAME to Constants.Tables.WORK_ITEMS,
                tableNames.TYPE to "TABLE",
                tableNames.REMARKS to "Work Item",
                tableNames.TYPES_CATALOG to null,
                tableNames.TYPES_SCHEMA to null,
                tableNames.TYPES_NAME to null,
                tableNames.SELF_REFERENCING_COL_NAME to null,
                tableNames.REF_GENERATION to null,
            ),
            mapOf(
                tableNames.CATALOG to null,
                tableNames.SCHEMA to null,
                tableNames.NAME to Constants.Tables.WORK_ITEM_LINKS,
                tableNames.TYPE to "TABLE",
                tableNames.REMARKS to "Work Item Links",
                tableNames.TYPES_CATALOG to null,
                tableNames.TYPES_SCHEMA to null,
                tableNames.TYPES_NAME to null,
                tableNames.SELF_REFERENCING_COL_NAME to null,
                tableNames.REF_GENERATION to null,
            ),
        )
        val cols = listOf(
            ColumnInfo(
                "",
                tableNames.CATALOG,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo("", tableNames.SCHEMA, Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo, 1024),
            ColumnInfo("", tableNames.NAME, Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo, 1024),
            ColumnInfo("", tableNames.TYPE, Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo, 1024),
            ColumnInfo(
                "",
                tableNames.REMARKS,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                tableNames.TYPES_CATALOG,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                tableNames.TYPES_SCHEMA,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                tableNames.TYPES_NAME,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                tableNames.SELF_REFERENCING_COL_NAME,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                tableNames.REF_GENERATION,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
        );
        return StringResultSet(WiqlStatement(this.connection), rows, cols)
    }

    override fun getSchemas(): ResultSet {
        logger.trace { "getSchemas  " }
        return emptyResultSet()
    }

    override fun getSchemas(catalog: String?, schemaPattern: String?): ResultSet {
        logger.trace { "getSchemas $catalog $schemaPattern " }
        return emptyResultSet()
    }

    override fun getCatalogs(): ResultSet {
        logger.trace { "getCatalogs  " }
        return emptyResultSet()
    }

    override fun getTableTypes(): ResultSet {
        logger.trace { "getTableTypes  " }
        val tableNames = Constants.JdbcTableColumns;
        val rows = listOf(
            mapOf(
                tableNames.TYPE to "TABLE",
            )
        )
        val cols = listOf(
            ColumnInfo("", tableNames.TYPE, Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo, 1024)
        );
        return StringResultSet(WiqlStatement(this.connection), rows, cols)
    }


    private val columnTypes = Constants.AdoColumnTypes

    private fun validColumn(field: Field): Boolean {
        logger.trace { "validColumn $field " }
        val result = field.isQueryable == true
        logger.trace { "validColumn result=$result " }
        return result
    }

    override fun getColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        logger.trace { "getColumns $catalog $schemaPattern $tableNamePattern $columnNamePattern " }
        val columnNames = Constants.JdbcColumnColumns;
        val fields = this.workItemsService.getFields(true)
        val rows = fields.listFieldsResponse.value
            .filter { f ->
                val table = Constants.adoUsageToTableName[f.usage]
                if (tableNamePattern == "%") {
                    true
                } else tableNamePattern == table
            }
            .filter { f -> validColumn(f) }
            .mapIndexed { i, f ->
                logger.trace { "loop field=$f" }
                val (dataType, typeName, size) = Constants.adoFieldToType[f.type]
                    ?: throw SQLException("Could not find a mapping for ${f.type}")
                val tableName = Constants.adoUsageToTableName[f.usage]
                mapOf(
                    columnNames.TABLE_CATALOG to JsonPrimitive(""),
                    columnNames.TABLE_SCHEMA to JsonPrimitive(""),
                    columnNames.TABLE_NAME to JsonPrimitive(tableName),
                    columnNames.COLUMN_NAME to JsonPrimitive(f.referenceName),
                    columnNames.DATA_TYPE to JsonPrimitive(dataType),
                    columnNames.TYPE_NAME to JsonPrimitive(typeName),
                    columnNames.COLUMN_SIZE to JsonPrimitive(size),
                    columnNames.BUFFER_LENGTH to JsonPrimitive(0),
                    columnNames.DECIMAL_DIGITS to JsonPrimitive(0),
                    columnNames.NUM_PREC_RADIX to JsonPrimitive(10),
                    columnNames.NULLABLE to JsonPrimitive(DatabaseMetaData.columnNullableUnknown),
                    columnNames.REMARKS to JsonPrimitive("Name: ${f.name} \n${f.description ?: ""}"),
                    columnNames.COLUMN_DEF to JsonPrimitive(""),
                    columnNames.SQL_DATA_TYPE to JsonPrimitive(0),
                    columnNames.SQL_DATETIME_SUB to JsonPrimitive(0),
                    columnNames.CHAR_OCTET_LENGTH to JsonPrimitive(Integer.MAX_VALUE),
                    columnNames.ORDINAL_POSITION to JsonPrimitive(i + 1),
                    columnNames.IS_NULLABLE to JsonPrimitive(""),
                    columnNames.SCOPE_CATALOG to JsonPrimitive(""),
                    columnNames.SCOPE_SCHEMA to JsonPrimitive(""),
                    columnNames.SCOPE_TABLE to JsonPrimitive(""),
                    columnNames.SOURCE_DATA_TYPE to JsonPrimitive(0),
                    columnNames.IS_AUTOINCREMENT to JsonPrimitive(Constants.JdbcColumnColumns.YesNo.No),
                    columnNames.IS_GENERATEDCOLUMN to JsonPrimitive(Constants.JdbcColumnColumns.YesNo.No),
                )
            }
        val cols = listOf(
            ColumnInfo(
                "",
                columnNames.TABLE_CATALOG,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.TABLE_SCHEMA,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.TABLE_NAME,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.COLUMN_NAME,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.DATA_TYPE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.TYPE_NAME,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.COLUMN_SIZE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.BUFFER_LENGTH,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.DECIMAL_DIGITS,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.NUM_PREC_RADIX,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.NULLABLE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.REMARKS,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.COLUMN_DEF,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.SQL_DATA_TYPE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.SQL_DATETIME_SUB,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.CHAR_OCTET_LENGTH,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.ORDINAL_POSITION,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.IS_NULLABLE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.SCOPE_CATALOG,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.SCOPE_SCHEMA,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.SCOPE_TABLE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.SOURCE_DATA_TYPE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.IS_AUTOINCREMENT,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.IS_GENERATEDCOLUMN,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
        )
        return JsonResultSet(WiqlStatement(this.connection), rows, cols)
    }

    override fun getColumnPrivileges(
        catalog: String?,
        schema: String?,
        table: String?,
        columnNamePattern: String?
    ): ResultSet {
        return emptyResultSet()
    }

    override fun getTablePrivileges(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet {
        logger.trace { "getTablePrivileges $catalog $schemaPattern $tableNamePattern " }
        return emptyResultSet()
    }

    override fun getBestRowIdentifier(
        catalog: String?,
        schema: String?,
        table: String?,
        scope: Int,
        nullable: Boolean
    ): ResultSet {
        val rows = listOf<Map<String, JsonElement>>()
        val cols = listOf<ColumnInfo>()
        return JsonResultSet(WiqlStatement(this.connection), rows, cols)
    }

    override fun getVersionColumns(catalog: String?, schema: String?, table: String?): ResultSet {
        logger.trace { "getVersionColumns $catalog $schema $table " }
        return emptyResultSet()
    }

    override fun getPrimaryKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        logger.trace { "getPrimaryKeys $catalog $schema $table " }
        return emptyResultSet()
    }

    override fun getImportedKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        logger.trace { "getImportedKeys $catalog $schema $table " }
        return emptyResultSet()
    }

    override fun getExportedKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        logger.trace { "getExportedKeys $catalog $schema $table " }
        return emptyResultSet()
    }

    override fun getCrossReference(
        parentCatalog: String?,
        parentSchema: String?,
        parentTable: String?,
        foreignCatalog: String?,
        foreignSchema: String?,
        foreignTable: String?
    ): ResultSet {
        return emptyResultSet()
    }

    override fun getTypeInfo(): ResultSet {
        logger.trace { "getTypeInfo  " }
        val columnNames = Constants.JdbcTypeColumns;
        val rows = Constants.adoFieldToType.keys
            .map { k ->
                val (type, name, size) = Constants.adoFieldToType[k] ?: throw SQLException("Bad mapping?")
                mapOf(
                    columnNames.TYPE_NAME to JsonPrimitive(name),
                    columnNames.DATA_TYPE to JsonPrimitive(type),
                    columnNames.PRECISION to JsonPrimitive(size),
                    columnNames.LITERAL_PREFIX to JsonPrimitive(null as String?),
                    columnNames.LITERAL_SUFFIX to JsonPrimitive(null as String?),
                    columnNames.CREATE_PARAMS to JsonPrimitive(null as String?),
                    columnNames.NULLABLE to JsonPrimitive(DatabaseMetaData.typeNullableUnknown),
                    columnNames.CASE_SENSITIVE to JsonPrimitive(false),
                    columnNames.SEARCHABLE to JsonPrimitive(DatabaseMetaData.typeSearchable),
                    columnNames.UNSIGNED_ATTRIBUTE to JsonPrimitive(false),
                    columnNames.FIXED_PREC_SCALE to JsonPrimitive(false),
                    columnNames.AUTO_INCREMENT to JsonPrimitive(false),
                    columnNames.LOCAL_TYPE_NAME to JsonPrimitive(null as String?),
                    columnNames.MINIMUM_SCALE to JsonPrimitive(0),
                    columnNames.MAXIMUM_SCALE to JsonPrimitive(Integer.MAX_VALUE),
                    columnNames.SQL_DATA_TYPE to JsonPrimitive(0),
                    columnNames.SQL_DATETIME_SUB to JsonPrimitive(0),
                    columnNames.NUM_PREC_RADIX to JsonPrimitive(10),
                )
            }
        val cols = listOf(
            ColumnInfo(
                "",
                columnNames.TYPE_NAME,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.DATA_TYPE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.PRECISION,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.LITERAL_PREFIX,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.LITERAL_SUFFIX,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.CREATE_PARAMS,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.NULLABLE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.CASE_SENSITIVE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.BOOLEAN] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.SEARCHABLE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.UNSIGNED_ATTRIBUTE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.BOOLEAN] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.FIXED_PREC_SCALE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.BOOLEAN] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.AUTO_INCREMENT,
                Constants.adoFieldToType[Constants.AdoColumnTypes.BOOLEAN] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.LOCAL_TYPE_NAME,
                Constants.adoFieldToType[Constants.AdoColumnTypes.STRING] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.MINIMUM_SCALE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.MAXIMUM_SCALE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.SQL_DATA_TYPE,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.SQL_DATETIME_SUB,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
            ColumnInfo(
                "",
                columnNames.NUM_PREC_RADIX,
                Constants.adoFieldToType[Constants.AdoColumnTypes.INTEGER] as TypeInfo,
                1024
            ),
        );
        return JsonResultSet(WiqlStatement(this.connection), rows, cols)
    }

    override fun getIndexInfo(
        catalog: String?,
        schema: String?,
        table: String?,
        unique: Boolean,
        approximate: Boolean
    ): ResultSet {
        return emptyResultSet()
    }

    override fun supportsResultSetType(type: Int): Boolean {
        logger.trace { "supportsResultSetType $type " }
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean {
        logger.trace { "supportsResultSetConcurrency $type $concurrency " }
        return false
    }

    override fun ownUpdatesAreVisible(type: Int): Boolean {
        logger.trace { "ownUpdatesAreVisible $type " }
        return false
    }

    override fun ownDeletesAreVisible(type: Int): Boolean {
        logger.trace { "ownDeletesAreVisible $type " }
        return false
    }

    override fun ownInsertsAreVisible(type: Int): Boolean {
        logger.trace { "ownInsertsAreVisible $type " }
        return false
    }

    override fun othersUpdatesAreVisible(type: Int): Boolean {
        logger.trace { "othersUpdatesAreVisible $type " }
        return false
    }

    override fun othersDeletesAreVisible(type: Int): Boolean {
        logger.trace { "othersDeletesAreVisible $type " }
        return false
    }

    override fun othersInsertsAreVisible(type: Int): Boolean {
        logger.trace { "othersInsertsAreVisible $type " }
        return false
    }

    override fun updatesAreDetected(type: Int): Boolean {
        logger.trace { "updatesAreDetected $type " }
        return false
    }

    override fun deletesAreDetected(type: Int): Boolean {
        logger.trace { "deletesAreDetected $type " }
        return false
    }

    override fun insertsAreDetected(type: Int): Boolean {
        logger.trace { "insertsAreDetected $type " }
        return false
    }

    override fun supportsBatchUpdates(): Boolean {
        logger.trace { "supportsBatchUpdates  " }
        return false
    }

    override fun getUDTs(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        types: IntArray?
    ): ResultSet {
        return emptyResultSet()
    }

    override fun getConnection(): Connection {
        logger.trace { "getConnection  " }
        return this.connection
    }

    override fun supportsSavepoints(): Boolean {
        logger.trace { "supportsSavepoints  " }
        return false
    }

    override fun supportsNamedParameters(): Boolean {
        logger.trace { "supportsNamedParameters  " }
        return false
    }

    override fun supportsMultipleOpenResults(): Boolean {
        logger.trace { "supportsMultipleOpenResults  " }
        return false
    }

    override fun supportsGetGeneratedKeys(): Boolean {
        logger.trace { "supportsGetGeneratedKeys  " }
        return false
    }

    override fun getSuperTypes(catalog: String?, schemaPattern: String?, typeNamePattern: String?): ResultSet {
        logger.trace { "getSuperTypes $catalog $schemaPattern $typeNamePattern " }
        return emptyResultSet()
    }

    override fun getSuperTables(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet {
        logger.trace { "getSuperTables $catalog $schemaPattern $tableNamePattern " }
        return emptyResultSet()
    }

    override fun getAttributes(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        attributeNamePattern: String?
    ): ResultSet {
        return emptyResultSet()
    }

    override fun supportsResultSetHoldability(holdability: Int): Boolean {
        logger.trace { "supportsResultSetHoldability $holdability " }
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    override fun getResultSetHoldability(): Int {
        logger.trace { "getResultSetHoldability  " }
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    override fun getDatabaseMajorVersion(): Int {
        logger.trace { "getDatabaseMajorVersion  " }
        return Constants.DB_VERSION_MAJOR
    }

    override fun getDatabaseMinorVersion(): Int {
        logger.trace { "getDatabaseMinorVersion  " }
        return Constants.DB_VERSION_MINOR
    }

    override fun getJDBCMajorVersion(): Int {
        logger.trace { "getJDBCMajorVersion  " }
        return 4
    }

    override fun getJDBCMinorVersion(): Int {
        logger.trace { "getJDBCMinorVersion  " }
        return 0
    }

    override fun getSQLStateType(): Int {
        logger.trace { "getSQLStateType  " }
        return DatabaseMetaData.sqlStateSQL;
    }

    override fun locatorsUpdateCopy(): Boolean {
        logger.trace { "locatorsUpdateCopy  " }
        return false
    }

    override fun supportsStatementPooling(): Boolean {
        logger.trace { "supportsStatementPooling  " }
        return false
    }

    override fun getRowIdLifetime(): RowIdLifetime? {
        logger.trace { "getRowIdLifetime  " }
        return null
    }

    override fun supportsStoredFunctionsUsingCallSyntax(): Boolean {
        logger.trace { "supportsStoredFunctionsUsingCallSyntax  " }
        return false
    }

    override fun autoCommitFailureClosesAllResultSets(): Boolean {
        logger.trace { "autoCommitFailureClosesAllResultSets  " }
        return false
    }

    override fun getClientInfoProperties(): ResultSet {
        logger.trace { "getClientInfoProperties  " }
        return emptyResultSet()
    }

    override fun getFunctions(catalog: String?, schemaPattern: String?, functionNamePattern: String?): ResultSet {
        logger.trace { "getFunctions $catalog $schemaPattern $functionNamePattern " }
        return emptyResultSet()
    }

    override fun getFunctionColumns(
        catalog: String?,
        schemaPattern: String?,
        functionNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        return emptyResultSet()
    }

    override fun getPseudoColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        return emptyResultSet()
    }

    override fun generatedKeyAlwaysReturned(): Boolean {
        logger.trace { "generatedKeyAlwaysReturned  " }
        return false
    }
}
