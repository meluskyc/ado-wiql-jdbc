import mu.KotlinLogging
import java.sql.*

class AdoJdbcResultSetMetaData (
    private val columns: List<ColumnInfo>
) : java.sql.ResultSetMetaData {
    private val logger = KotlinLogging.logger {}

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        logger.trace { "unwrap $iface " }
        if (iface == null) {
            throw SQLException("Couldn't unwrap")
        }
        try {
            return iface.cast(this)
        } catch (cce: ClassCastException) {
            throw SQLException("Couldn't unwrap $iface")
        }
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        logger.trace { "isWrapperFor $iface " }
        return iface != null && iface.isInstance(this);
    }

    override fun getColumnCount(): Int {
        logger.trace { "getColumnCount  " }
        return columns.size
    }

    override fun isAutoIncrement(column: Int): Boolean {
        logger.trace { "isAutoIncrement $column " }
        return false
    }

    override fun isCaseSensitive(column: Int): Boolean {
        logger.trace { "isCaseSensitive $column " }
        return false
    }

    override fun isSearchable(column: Int): Boolean {
        logger.trace { "isSearchable $column " }
        return true
    }

    override fun isCurrency(column: Int): Boolean {
        logger.trace { "isCurrency $column " }
        return false
    }

    override fun isNullable(column: Int): Int {
        logger.trace { "isNullable $column " }
        return ResultSetMetaData.columnNoNulls
    }

    override fun isSigned(column: Int): Boolean {
        logger.trace { "isSigned $column " }
        return true
    }

    override fun getColumnDisplaySize(column: Int): Int {
        logger.trace { "getColumnDisplaySize $column " }
        return 1024
    }

    override fun getColumnLabel(column: Int): String {
        logger.trace { "getColumnLabel $column " }
        val res = columns[column - 1].name;
        return res
    }

    override fun getColumnName(column: Int): String {
        logger.trace { "getColumnName $column " }
        val res = columns[column - 1].name;
        return res
    }

    override fun getSchemaName(column: Int): String {
        logger.trace { "getSchemaName $column " }
        return ""
    }

    override fun getPrecision(column: Int): Int {
        logger.trace { "getPrecision $column " }
        return 1024
    }

    override fun getScale(column: Int): Int {
        logger.trace { "getScale $column " }
        return 0
    }

    override fun getTableName(column: Int): String {
        logger.trace { "getTableName $column " }
        return ""
    }

    override fun getCatalogName(column: Int): String {
        logger.trace { "getCatalogName $column " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getColumnType(column: Int): Int {
        logger.trace { "getColumnType $column " }
        val col = columns[column - 1];
        return col.type.sqlType;
    }

    override fun getColumnTypeName(column: Int): String {
        logger.trace { "getColumnTypeName $column " }
        val col = columns[column - 1];
        return col.type.name;
    }

    override fun isReadOnly(column: Int): Boolean {
        logger.trace { "isReadOnly $column " }
        return true
    }

    override fun isWritable(column: Int): Boolean {
        logger.trace { "isWritable $column " }
        return false
    }

    override fun isDefinitelyWritable(column: Int): Boolean {
        logger.trace { "isDefinitelyWritable $column " }
        return false
    }

    override fun getColumnClassName(column: Int): String {
        logger.trace { "getColumnClassName $column " }
        val col = columns[column - 1]
        val res = when (col.type.sqlType) {
            Types.NVARCHAR -> String::class.java.name
            Types.INTEGER -> Integer::class.java.name // doesn't work? :(
            Types.DOUBLE -> Double::class.java.name
            Types.DATE -> Date::class.java.name
            Types.TIMESTAMP -> Timestamp::class.java.name
            Types.BOOLEAN -> Boolean::class.java.name
            else -> throw SQLException("didn't find a mapping for $col")
        }
        return res
    }
}
