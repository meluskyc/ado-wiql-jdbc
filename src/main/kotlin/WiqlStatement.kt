import com.google.gson.JsonElement
import mu.KotlinLogging
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Date
import java.sql.Statement.CLOSE_CURRENT_RESULT
import java.util.*


fun canIgnoreStatement(sql: String?): Boolean {
    if (sql?.contains("FROM", true) == false) {
        return true
    }
    return false
}

class WiqlStatement(
    private val adoJdbcConnection: AdoJdbcConnection,
    private val preparedSql: String = ""
) : Statement, PreparedStatement {
    private var isClosed = false
    private var resultSet: JsonResultSet? = null
    private var fetchSize = 1
    private val logger = KotlinLogging.logger {}
    private val workItemsService = this.adoJdbcConnection.workItemsService

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        logger.trace { "unwrap $iface " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        logger.trace { "isWrapperFor $iface " }
        return false
    }

    override fun close() {
        logger.trace { "close  " }
        isClosed = true
    }

    override fun executeQuery(): ResultSet {
        logger.trace { "executeQuery  " }
        if (canIgnoreStatement(this.preparedSql)) {
            return emptyResultSet()
        }
        return executeQuery(this.preparedSql)
    }

    private fun checkClosed() {
        logger.trace { "checkClosed  " }
        if (isClosed) {
            throw SQLException("Statement is closed")
        }
    }

    private fun emptyResultSet(): ResultSet {
        val rows = listOf<Map<String, JsonElement>>()
        val cols = listOf<ColumnInfo>()
        return JsonResultSet(WiqlStatement(this.adoJdbcConnection), rows, cols)
    }

    override fun executeQuery(sql: String?): ResultSet {
        logger.trace { "executeQuery $sql " }
        this.checkClosed()

        if (canIgnoreStatement(sql)) {
            return emptyResultSet();
        }

        val res = workItemsService.executeWiql(sql ?: "")
        val fieldsMap = workItemsService.getFieldsMap()
        val rows = res.workItemsResponse.value.map { v -> v.fields }
        val cols = res.wiqlResponse.columns.map { c ->
            fieldsMap[c.referenceName] ?: throw SQLException("Could not find type info for ${c.referenceName}")
        }
        return JsonResultSet(this, rows, cols)
    }

    override fun executeUpdate(): Int {
        logger.trace { "executeUpdate " }
        return 0
    }

    override fun executeUpdate(sql: String?): Int {
        logger.trace { "executeUpdate $sql " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun executeUpdate(sql: String?, autoGeneratedKeys: Int): Int {
        logger.trace { "executeUpdate $sql $autoGeneratedKeys " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun executeUpdate(sql: String?, columnIndexes: IntArray?): Int {
        logger.trace { "executeUpdate $sql $columnIndexes " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun executeUpdate(sql: String?, columnNames: Array<out String>?): Int {
        logger.trace { "executeUpdate $sql $columnNames " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getMaxFieldSize(): Int {
        logger.trace { "getMaxFieldSize  " }
        this.checkClosed()
        return Integer.MAX_VALUE
    }

    override fun setMaxFieldSize(max: Int) {
        logger.trace { "setMaxFieldSize $max " }
        this.checkClosed()
    }

    override fun getMaxRows(): Int {
        logger.trace { "getMaxRows  " }
        this.checkClosed()
        return 0
    }

    override fun setMaxRows(max: Int) {
        logger.trace { "setMaxRows $max " }
        this.checkClosed()
    }

    override fun setEscapeProcessing(enable: Boolean) {
        logger.trace { "setEscapeProcessing $enable " }
        this.checkClosed()
    }

    override fun getQueryTimeout(): Int {
        logger.trace { "getQueryTimeout  " }
        this.checkClosed()
        return 0
    }

    override fun setQueryTimeout(seconds: Int) {
        logger.trace { "setQueryTimeout $seconds " }
    }

    override fun cancel() {
        logger.trace { "cancel  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getWarnings(): SQLWarning {
        logger.trace { "getWarnings  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun clearWarnings() {
        logger.trace { "clearWarnings  " }
        this.checkClosed()
    }

    override fun setCursorName(name: String?) {
        logger.trace { "setCursorName $name " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun execute(): Boolean {
        logger.trace { "execute " }
        return false
    }

    override fun execute(sql: String?): Boolean {
        logger.trace { "execute $sql " }
        this.checkClosed()

        if (canIgnoreStatement(sql)) {
            return false
        }

        val res = workItemsService.executeWiql(sql ?: "")
        val fieldsMap = workItemsService.getFieldsMap()
        val rows = res.workItemsResponse.value.map { v -> v.fields }
        val cols = res.wiqlResponse.columns.map { c ->
            fieldsMap[c.referenceName] ?: throw SQLException("Could not find type info for ${c.referenceName}")
        }
        this.resultSet = JsonResultSet(this, rows, cols)
        return true
    }

    override fun execute(sql: String?, autoGeneratedKeys: Int): Boolean {
        logger.trace { "execute $sql $autoGeneratedKeys " }
        this.checkClosed()
        if (canIgnoreStatement(sql)) {
            return false
        }
        val res = workItemsService.executeWiql(sql ?: "")
        val fieldsMap = workItemsService.getFieldsMap()
        val rows = res.workItemsResponse.value.map { v -> v.fields }
        val cols = res.wiqlResponse.columns.map { c ->
            fieldsMap[c.referenceName] ?: throw SQLException("Could not find type info for ${c.referenceName}")
        }
        this.resultSet = JsonResultSet(this, rows, cols)
        return true
    }

    override fun execute(sql: String?, columnIndexes: IntArray?): Boolean {
        logger.trace { "execute $sql $columnIndexes " }
        this.checkClosed()
        if (canIgnoreStatement(sql)) {
            return false
        }
        val res = workItemsService.executeWiql(sql ?: "")
        val fieldsMap = workItemsService.getFieldsMap()
        val rows = res.workItemsResponse.value.map { v -> v.fields }
        val cols = res.wiqlResponse.columns.map { c ->
            fieldsMap[c.referenceName] ?: throw SQLException("Could not find type info for ${c.referenceName}")
        }
        this.resultSet = JsonResultSet(this, rows, cols)
        return true
    }

    override fun execute(sql: String?, columnNames: Array<out String>?): Boolean {
        logger.trace { "execute $sql $columnNames " }
        this.checkClosed()

        if (canIgnoreStatement(sql)) {
            return false
        }
        val res = workItemsService.executeWiql(sql ?: "")
        val fieldsMap = workItemsService.getFieldsMap()
        val rows = res.workItemsResponse.value.map { v -> v.fields }
        val cols = res.wiqlResponse.columns.map { c ->
            fieldsMap[c.referenceName] ?: throw SQLException("Could not find type info for ${c.referenceName}")
        }
        this.resultSet = JsonResultSet(this, rows, cols)
        return true
    }

    override fun getResultSet(): ResultSet {
        logger.trace { "getResultSet  " }
        return this.resultSet as ResultSet
    }

    override fun getUpdateCount(): Int {
        logger.trace { "getUpdateCount  " }
        this.checkClosed()
        return -1
    }

    override fun getMoreResults(): Boolean {
        logger.trace { "getMoreResults  " }
        this.checkClosed();
        return this.getMoreResults(CLOSE_CURRENT_RESULT);
    }

    override fun getMoreResults(current: Int): Boolean {
        logger.trace { "getMoreResults $current " }
        this.checkClosed();
        return false;
    }

    override fun setFetchDirection(direction: Int) {
        logger.trace { "setFetchDirection $direction " }
        this.checkClosed()
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getFetchDirection(): Int {
        logger.trace { "getFetchDirection  " }
        this.checkClosed()
        return ResultSet.FETCH_FORWARD
    }

    override fun setFetchSize(rows: Int) {
        logger.trace { "setFetchSize $rows " }
        this.checkClosed();
        this.fetchSize = rows
    }

    override fun getFetchSize(): Int {
        logger.trace { "getFetchSize  " }
        this.checkClosed();
        return this.fetchSize
    }

    override fun getResultSetConcurrency(): Int {
        logger.trace { "getResultSetConcurrency  " }
        this.checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    override fun getResultSetType(): Int {
        logger.trace { "getResultSetType  " }
        this.checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    override fun addBatch() {
        logger.trace { "addBatch " }
    }

    override fun addBatch(sql: String?) {
        logger.trace { "addBatch $sql " }
        this.checkClosed();
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun clearBatch() {
        logger.trace { "clearBatch  " }
        this.checkClosed();
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun executeBatch(): IntArray {
        logger.trace { "executeBatch  " }
        this.checkClosed();
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getConnection(): Connection {
        logger.trace { "getConnection  " }
        return this.adoJdbcConnection
    }

    override fun getGeneratedKeys(): ResultSet {
        logger.trace { "getGeneratedKeys  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getResultSetHoldability(): Int {
        logger.trace { "getResultSetHoldability  " }
        this.checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    override fun isClosed(): Boolean {
        logger.trace { "isClosed  " }
        return this.isClosed
    }

    override fun setPoolable(poolable: Boolean) {
        logger.trace { "setPoolable $poolable " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun isPoolable(): Boolean {
        logger.trace { "isPoolable  " }
        this.checkClosed()
        return false
    }

    override fun closeOnCompletion() {
        logger.trace { "closeOnCompletion  " }
        this.checkClosed()
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun isCloseOnCompletion(): Boolean {
        logger.trace { "isCloseOnCompletion  " }
        this.checkClosed()
        return false
    }

    override fun setNull(parameterIndex: Int, sqlType: Int) {
        logger.trace { "setNull $parameterIndex $sqlType  " }
    }

    override fun setNull(parameterIndex: Int, sqlType: Int, typeName: String?) {
        logger.trace { "setNull $parameterIndex $sqlType $typeName " }
    }

    override fun setBoolean(parameterIndex: Int, x: Boolean) {
        logger.trace { "setBoolean $parameterIndex $x" }
    }

    override fun setByte(parameterIndex: Int, x: Byte) {
        logger.trace { "setByte $parameterIndex $x" }
    }

    override fun setShort(parameterIndex: Int, x: Short) {
        logger.trace { "setShort $parameterIndex $x" }
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        logger.trace { "setInt $parameterIndex $x" }
    }

    override fun setLong(parameterIndex: Int, x: Long) {
        logger.trace { "setLong $parameterIndex $x" }
    }

    override fun setFloat(parameterIndex: Int, x: Float) {
        logger.trace { "setFloat $parameterIndex $x" }
    }

    override fun setDouble(parameterIndex: Int, x: Double) {
        logger.trace { "setDouble $parameterIndex $x" }
    }

    override fun setBigDecimal(parameterIndex: Int, x: BigDecimal?) {
        logger.trace { "setBigDecimal $parameterIndex $x" }
    }

    override fun setString(parameterIndex: Int, x: String?) {
        logger.trace { "setString $parameterIndex $x" }
    }

    override fun setBytes(parameterIndex: Int, x: ByteArray?) {
        logger.trace { "setBytes $parameterIndex $x" }
    }

    override fun setDate(parameterIndex: Int, x: Date?) {
        logger.trace { "setDate $parameterIndex $x" }
    }

    override fun setDate(parameterIndex: Int, x: Date?, cal: Calendar?) {
        logger.trace { "setDate $parameterIndex $x $cal" }
    }

    override fun setTime(parameterIndex: Int, x: Time?) {
        logger.trace { "setTime $parameterIndex $x " }
    }

    override fun setTime(parameterIndex: Int, x: Time?, cal: Calendar?) {
        logger.trace { "setTime $parameterIndex $x $cal" }
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?) {
        logger.trace { "setTimestamp $parameterIndex $x " }
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?, cal: Calendar?) {
        logger.trace { "setTimestamp $parameterIndex $x $cal" }
    }

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Int) {
        logger.trace { "setAsciiStream $parameterIndex $x $length" }
    }

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Long) {
        logger.trace { "setAsciiStream $parameterIndex $x $length" }
    }

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?) {
        logger.trace { "setAsciiStream $parameterIndex $x " }
    }

    override fun setUnicodeStream(parameterIndex: Int, x: InputStream?, length: Int) {
        logger.trace { "setUnicodeStream $parameterIndex $x $length" }
    }

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Int) {
        logger.trace { "setBinaryStream $parameterIndex $x $length" }
    }

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Long) {
        logger.trace { "setBinaryStream $parameterIndex $x $length" }
    }

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?) {
        logger.trace { "setBinaryStream $parameterIndex $x " }
    }

    override fun clearParameters() {
        logger.trace { "clearParameters " }
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int) {
        logger.trace { "setObject $parameterIndex $x $targetSqlType" }
    }

    override fun setObject(parameterIndex: Int, x: Any?) {
        logger.trace { "setObject $parameterIndex $x " }
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int, scaleOrLength: Int) {
        logger.trace { "setObject $parameterIndex $x $targetSqlType $scaleOrLength" }
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Int) {
        logger.trace { "setCharacterStream $parameterIndex $reader $length" }
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Long) {
        logger.trace { "setCharacterStream $parameterIndex $reader $length" }
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?) {
        logger.trace { "setCharacterStream $parameterIndex $reader " }
    }

    override fun setRef(parameterIndex: Int, x: Ref?) {
        logger.trace { "setRef $parameterIndex $x " }
    }

    override fun setBlob(parameterIndex: Int, x: Blob?) {
        logger.trace { "setBlob $parameterIndex $x " }
    }

    override fun setBlob(parameterIndex: Int, inputStream: InputStream?, length: Long) {
        logger.trace { "setBlob $parameterIndex $inputStream $length " }
    }

    override fun setBlob(parameterIndex: Int, inputStream: InputStream?) {
        logger.trace { "setBlob $parameterIndex $inputStream " }
    }

    override fun setClob(parameterIndex: Int, x: Clob?) {
        logger.trace { "setClob $parameterIndex $x " }
    }

    override fun setClob(parameterIndex: Int, reader: Reader?, length: Long) {
        logger.trace { "setClob $parameterIndex $reader $length " }
    }

    override fun setClob(parameterIndex: Int, reader: Reader?) {
        logger.trace { "setClob $parameterIndex $reader " }
    }

    override fun setArray(parameterIndex: Int, x: java.sql.Array?) {
        logger.trace { "setArray $parameterIndex $x " }
    }

    override fun getMetaData(): java.sql.ResultSetMetaData {
        logger.trace { "getMetaData " }
        return AdoJdbcResultSetMetaData(listOf())
    }

    override fun setURL(parameterIndex: Int, x: URL?) {
        logger.trace { "setURL $parameterIndex $x " }
    }

    override fun getParameterMetaData(): ParameterMetaData {
        logger.trace { "getMetaData " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun setRowId(parameterIndex: Int, x: RowId?) {
        logger.trace { "parameterIndex $x " }
    }

    override fun setNString(parameterIndex: Int, value: String?) {
        logger.trace { "setNString $parameterIndex $value" }
    }

    override fun setNCharacterStream(parameterIndex: Int, value: Reader?, length: Long) {
        logger.trace { "setNCharacterStream $parameterIndex $value $length" }
    }

    override fun setNCharacterStream(parameterIndex: Int, value: Reader?) {
        logger.trace { "setNCharacterStream $parameterIndex $value" }
    }

    override fun setNClob(parameterIndex: Int, value: NClob?) {
        logger.trace { "setNClob $parameterIndex $value" }
    }

    override fun setNClob(parameterIndex: Int, reader: Reader?, length: Long) {
        logger.trace { "setNClob $parameterIndex $reader $length " }
    }

    override fun setNClob(parameterIndex: Int, reader: Reader?) {
        logger.trace { "setNClob $parameterIndex $reader " }
    }

    override fun setSQLXML(parameterIndex: Int, xmlObject: SQLXML?) {
        logger.trace { "setSQLXML $parameterIndex $xmlObject " }
    }
}
