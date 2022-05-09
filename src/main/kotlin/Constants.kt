data class TypeInfo(val sqlType: Int, val name: String, val maxSize: Int);
data class ColumnInfo(
    val tableName: String,
    val name: String,
    val type: TypeInfo,
    val size: Int,
    val isQueryable: Boolean?,
    val isIdentity: Boolean?
) {
    constructor(
        tableName: String,
        name: String,
        type: TypeInfo,
        size: Int
    ) :
            this(tableName, name, type, size, false, false)
}

class Constants {
    companion object {
        const val DRIVER_NAME = "ado-wiql-jdbc"
        const val DRIVER_VERSION = "1.0"
        const val DRIVER_VERSION_MAJOR = 1
        const val DRIVER_VERSION_MINOR = 0
        const val DB_VERSION_MAJOR = 1
        const val DB_VERSION_MINOR = 0

        const val PROPERTY_API_URL_BASE = "API_URL_BASE"
        const val PROPERTY_PROJECT = "PROJECT"
        const val PROPERTY_TEAM = "TEAM"
        const val PROPERTY_USER_NAME = "USERNAME"
        const val PROPERTY_PAT = "PAT"

        val adoFieldToType = mapOf(
            AdoColumnTypes.BOOLEAN to TypeInfo(
                java.sql.Types.BOOLEAN, AdoColumnTypes.BOOLEAN, Integer.MAX_VALUE
            ),
            AdoColumnTypes.DATETIME to TypeInfo(
                java.sql.Types.TIMESTAMP, AdoColumnTypes.DATETIME, Integer.MAX_VALUE
            ),
            AdoColumnTypes.DOUBLE to TypeInfo(
                java.sql.Types.DOUBLE, AdoColumnTypes.DOUBLE, Integer.MAX_VALUE
            ),
            AdoColumnTypes.HTML to TypeInfo(
                java.sql.Types.NVARCHAR, AdoColumnTypes.HTML, Integer.MAX_VALUE
            ),
            AdoColumnTypes.INTEGER to TypeInfo(
                java.sql.Types.INTEGER, AdoColumnTypes.INTEGER, Integer.MAX_VALUE
            ),
            AdoColumnTypes.PLAIN_TEXT to TypeInfo(
                java.sql.Types.NVARCHAR, AdoColumnTypes.PLAIN_TEXT, Integer.MAX_VALUE
            ),
            AdoColumnTypes.STRING to TypeInfo(
                java.sql.Types.NVARCHAR, AdoColumnTypes.STRING, Integer.MAX_VALUE
            ),
            AdoColumnTypes.TREE_PATH to TypeInfo(
                java.sql.Types.NVARCHAR, AdoColumnTypes.TREE_PATH, Integer.MAX_VALUE
            ),
            AdoColumnTypes.HISTORY to TypeInfo(
                java.sql.Types.NVARCHAR, AdoColumnTypes.HISTORY, Integer.MAX_VALUE
            ),
            AdoColumnTypes.GUID to TypeInfo(
                java.sql.Types.NVARCHAR, AdoColumnTypes.GUID, Integer.MAX_VALUE
            ),
            AdoColumnTypes.IDENTITY to TypeInfo( // ?
                java.sql.Types.NVARCHAR, AdoColumnTypes.IDENTITY, Integer.MAX_VALUE
            ),
            AdoColumnTypes.PICKLIST_DOUBLE to TypeInfo(
                java.sql.Types.DOUBLE, AdoColumnTypes.PICKLIST_DOUBLE, Integer.MAX_VALUE
            ),
            AdoColumnTypes.PICKLIST_INTEGER to TypeInfo(
                java.sql.Types.INTEGER, AdoColumnTypes.PICKLIST_INTEGER, Integer.MAX_VALUE
            ),
            AdoColumnTypes.PICKLIST_STRING to TypeInfo(
                java.sql.Types.NVARCHAR, AdoColumnTypes.PICKLIST_STRING, Integer.MAX_VALUE
            ),
        )

        val adoUsageToTableName = mapOf(
            "workItem" to Tables.WORK_ITEMS,
            "workItemTypeExtension" to Tables.WORK_ITEMS,
            "workItemLink" to Tables.WORK_ITEM_LINKS,
        )
    }


    object Tables {
        const val WORK_ITEMS = "WorkItems"
        const val WORK_ITEM_LINKS = "WorkItemLinks"
    }

    object AdoColumnTypes {
        const val BOOLEAN = "boolean"
        const val DATETIME = "dateTime"
        const val DOUBLE = "double"
        const val GUID = "guid"
        const val HISTORY = "history"
        const val HTML = "html"
        const val IDENTITY = "identity"
        const val INTEGER = "integer"
        const val PICKLIST_DOUBLE = "picklistDouble"
        const val PICKLIST_INTEGER = "picklistInteger"
        const val PICKLIST_STRING = "picklistString"
        const val PLAIN_TEXT = "plainText"
        const val STRING = "string"
        const val TREE_PATH = "treePath"
    }

    /**
     * {@link java.sql.DatabaseMetaData getTables}
     */
    object JdbcTableColumns {
        const val CATALOG = "TABLE_CAT"
        const val SCHEMA = "TABLE_SCHEM"
        const val NAME = "TABLE_NAME"
        const val TYPE = "TABLE_TYPE"
        const val REMARKS = "REMARKS"
        const val TYPES_CATALOG = "TYPE_CAT"
        const val TYPES_SCHEMA = "TYPE_SCHEM"
        const val TYPES_NAME = "TYPE_NAME"
        const val SELF_REFERENCING_COL_NAME = "SELF_REFERENCING_COL_NAME"
        const val REF_GENERATION = "REF_GENERATION"
    }

    /**
     * {@link java.sql.DatabaseMetaData getColumns}
     */
    object JdbcColumnColumns {
        const val TABLE_CATALOG = "TABLE_CAT"
        const val TABLE_SCHEMA = "TABLE_SCHEM"
        const val TABLE_NAME = "TABLE_NAME"
        const val COLUMN_NAME = "COLUMN_NAME"
        const val DATA_TYPE = "DATA_TYPE"
        const val TYPE_NAME = "TYPE_NAME"
        const val COLUMN_SIZE = "COLUMN_SIZE"
        const val BUFFER_LENGTH = "BUFFER_LENGTH"
        const val DECIMAL_DIGITS = "DECIMAL_DIGITS"
        const val NUM_PREC_RADIX = "NUM_PREC_RADIX"
        const val NULLABLE = "NULLABLE"
        const val REMARKS = "REMARKS"
        const val COLUMN_DEF = "COLUMN_DEF"
        const val SQL_DATA_TYPE = "SQL_DATA_TYPE"
        const val SQL_DATETIME_SUB = "SQL_DATETIME_SUB"
        const val CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH"
        const val ORDINAL_POSITION = "ORDINAL_POSITION"
        const val IS_NULLABLE = "IS_NULLABLE"
        const val SCOPE_CATALOG = "SCOPE_CATALOG"
        const val SCOPE_SCHEMA = "SCOPE_SCHEMA"
        const val SCOPE_TABLE = "SCOPE_TABLE"
        const val SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE"
        const val IS_AUTOINCREMENT = "IS_AUTOINCREMENT"
        const val IS_GENERATEDCOLUMN = "IS_GENERATEDCOLUMN"

        object YesNo {
            const val Yes = "YES"
            const val No = "NO"
        }
    }

    /**
     * {@link java.sql.DatabaseMetaData getTypeInfo}
     */
    object JdbcTypeColumns {
        const val TYPE_NAME = "TYPE_NAME"
        const val DATA_TYPE = "DATA_TYPE"
        const val PRECISION = "PRECISION"
        const val LITERAL_PREFIX = "LITERAL_PREFIX"
        const val LITERAL_SUFFIX = "LITERAL_SUFFIX"
        const val CREATE_PARAMS = "CREATE_PARAMS"
        const val NULLABLE = "NULLABLE"
        const val CASE_SENSITIVE = "CASE_SENSITIVE"
        const val SEARCHABLE = "SEARCHABLE"
        const val UNSIGNED_ATTRIBUTE = "UNSIGNED_ATTRIBUTE"
        const val FIXED_PREC_SCALE = "FIXED_PREC_SCALE"
        const val AUTO_INCREMENT = "AUTO_INCREMENT"
        const val LOCAL_TYPE_NAME = "LOCAL_TYPE_NAME"
        const val MINIMUM_SCALE = "MINIMUM_SCALE"
        const val MAXIMUM_SCALE = "MAXIMUM_SCALE"
        const val SQL_DATA_TYPE = "SQL_DATA_TYPE"
        const val SQL_DATETIME_SUB = "SQL_DATETIME_SUB"
        const val NUM_PREC_RADIX = "NUM_PREC_RADIX"
    }

}
