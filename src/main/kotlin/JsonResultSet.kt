import adoApi.displayIdentityColumn
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import mu.KotlinLogging
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.MalformedURLException
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.sql.ResultSet.*
import java.time.Instant
import java.util.*

class JsonResultSet(
    private val statement: WiqlStatement,
    private val rows: List<Map<String, com.google.gson.JsonElement>>,
    private val columns: List<ColumnInfo>
) : ResultSet {

    private val columnIndexesByLabel = columns
        .mapIndexed { i, c -> Pair(i, c.name) }
        .associateBy { (_, label) -> label }
        .mapValues { (_, value) -> value.first }
    private var rowIndex = -1
    private var isClosed = false
    private val logger = KotlinLogging.logger {}

    private fun checkClosed() {
        logger.trace { "checkClosed  " }
        if (isClosed) {
            throw SQLException("Result set is closed.")
        }
    }

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        logger.trace { "unwrap $iface " }
        if (iface == null) {
            throw SQLException("Couldn't unwrap")
        }
        try {
            return iface.cast(this)
        } catch (cce: ClassCastException) {
            throw SQLException("Couldn't unwrap")
        }
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        logger.trace { "isWrapperFor $iface " }
        return iface != null && iface.isInstance(this)
    }

    override fun close() {
        logger.trace { "close  " }
        this.isClosed = true
    }

    override fun next(): Boolean {
        logger.trace { "next  " }
        checkClosed()

        return if (rowIndex < this.rows.size - 1) {
            logger.trace { "next rowIndex++ " }
            rowIndex++
            true
        } else {
            logger.trace { "next false " }
            false
        }
    }

    override fun wasNull(): Boolean {
        logger.trace { "wasNull  " }
        checkClosed()
        return false
    }

    override fun getString(columnIndex: Int): String {
        logger.trace { "getString $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive("")

        if (col.isIdentity == true && v is JsonObject) {
            return v.displayIdentityColumn()
        }

        return v.asString
    }

    override fun getString(columnLabel: String?): String {
        logger.trace { "getString $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive("")
        val colIx = columnIndexesByLabel[columnLabel] ?: -1
        val col = columns[colIx]

        if (col.isIdentity == true && v is JsonObject) {
            return v.displayIdentityColumn()
        }

        return v.asString
    }

    override fun getBoolean(columnIndex: Int): Boolean {
        logger.trace { "getBoolean $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive(false)
        return v.asBoolean
    }

    override fun getBoolean(columnLabel: String?): Boolean {
        logger.trace { "getBoolean $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(false)
        return v.asBoolean
    }

    override fun getByte(columnIndex: Int): Byte {
        logger.trace { "getByte $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive(false)
        return v.asByte
    }

    override fun getByte(columnLabel: String?): Byte {
        logger.trace { "getByte $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(false)
        return v.asByte
    }

    override fun getShort(columnIndex: Int): Short {
        logger.trace { "getShort $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive(0)
        return v.asShort
    }

    override fun getShort(columnLabel: String?): Short {
        logger.trace { "getShort $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(0)
        return v.asShort
    }

    override fun getInt(columnIndex: Int): Int {
        logger.trace { "getInt $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive(0)
        return v.asInt
    }

    override fun getInt(columnLabel: String?): Int {
        logger.trace { "getInt $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(0)
        return v.asInt
    }

    override fun getLong(columnIndex: Int): Long {
        logger.trace { "getLong $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive(0)
        return v.asLong
    }

    override fun getLong(columnLabel: String?): Long {
        logger.trace { "getLong $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(0)
        return v.asLong
    }

    override fun getFloat(columnIndex: Int): Float {
        logger.trace { "getFloat $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive(0)
        return v.asFloat
    }

    override fun getFloat(columnLabel: String?): Float {
        logger.trace { "getFloat $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(0)
        return v.asFloat
    }

    override fun getDouble(columnIndex: Int): Double {
        logger.trace { "getDouble $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive(0)
        return v.asDouble
    }

    override fun getDouble(columnLabel: String?): Double {
        logger.trace { "getDouble $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(0)
        return v.asDouble
    }

    @Deprecated("Deprecated in Java")
    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal {
        logger.trace { "getBigDecimal $columnIndex $scale " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive(0)
        return v.asBigDecimal
    }

    @Deprecated("Deprecated in Java")
    override fun getBigDecimal(columnLabel: String?, scale: Int): BigDecimal {
        logger.trace { "getBigDecimal $columnLabel $scale " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(0)
        return v.asBigDecimal
    }

    override fun getBigDecimal(columnIndex: Int): BigDecimal {
        logger.trace { "getBigDecimal $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive(0)
        return v.asBigDecimal
    }

    override fun getBigDecimal(columnLabel: String?): BigDecimal {
        logger.trace { "getBigDecimal $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(0)
        return v.asBigDecimal
    }

    override fun getBytes(columnIndex: Int): ByteArray {
        logger.trace { "getBytes $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive(0)
        return v.asString.toByteArray()
    }

    override fun getBytes(columnLabel: String?): ByteArray {
        logger.trace { "getBytes $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(0)
        return v.asString.toByteArray()
    }

    override fun getDate(columnIndex: Int): Date? {
        logger.trace { "getDate $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name]
        if (v != null) {
            return Date(Date.from(Instant.parse(v.asString)).time)
        }
        return null
    }

    override fun getDate(columnLabel: String?): Date? {
        logger.trace { "getDate $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel]
        if (v != null) {
            return Date(Date.from(Instant.parse(v.asString)).time)
        }
        return null
    }

    override fun getDate(columnIndex: Int, cal: Calendar?): Date? {
        logger.trace { "getDate $columnIndex $cal " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name]
        if (v != null) {
            val date = Date.from(Instant.parse(v.asString)) as Date
            cal?.time = date
            return date
        }
        return null
    }

    override fun getDate(columnLabel: String?, cal: Calendar?): Date? {
        logger.trace { "getDate $columnLabel $cal " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel]
        if (v != null) {
            val date = Date.from(Instant.parse(v.asString)) as Date
            cal?.time = date
            return date
        }
        return null
    }

    override fun getTime(columnIndex: Int): Time {
        logger.trace { "getTime $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive(0)
        val inst = Instant.parse(v.asString) as Instant
        return Time(inst.epochSecond)
    }

    override fun getTime(columnLabel: String?): Time {
        logger.trace { "getTime $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(0)
        val inst = Instant.parse(v.asString) as Instant
        return Time(inst.epochSecond)
    }

    override fun getTime(columnIndex: Int, cal: Calendar?): Time {
        logger.trace { "getTime $columnIndex $cal " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: JsonPrimitive(0)
        val date = Date.from(Instant.parse(v.asString)) as Date
        cal?.time = date
        if (cal == null) {
            return Time(date.time)
        }
        return Time(cal.time.time)
    }

    override fun getTime(columnLabel: String?, cal: Calendar?): Time {
        logger.trace { "getTime $columnLabel $cal " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(0)
        val date = Date.from(Instant.parse(v.asString)) as Date
        cal?.time = date
        if (cal == null) {
            return Time(date.time)
        }
        return Time(cal.time.time)
    }

    override fun getTimestamp(columnIndex: Int): Timestamp? {
        logger.trace { "getTimestamp $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name]
        if (v != null) {
            val res = Timestamp.from(Instant.parse(v.asString))
            return res
        }
        return null
    }

    override fun getTimestamp(columnLabel: String?): Timestamp? {
        logger.trace { "getTimestamp $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: JsonPrimitive(0)
        val res = Timestamp.from(Instant.parse(v.asString))
        return res
    }

    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp? {
        logger.trace { "getTimestamp $columnIndex $cal " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name]
        if (v != null) {
            val date = Timestamp.from(Instant.parse(v.asString))
            cal?.time = date
            if (cal == null) {
                return Timestamp(date.time)
            }
            return Timestamp(cal.time.time)
        }
        return null
    }

    override fun getTimestamp(columnLabel: String?, cal: Calendar?): Timestamp? {
        logger.trace { "getTimestamp $columnLabel $cal " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel]
        if (v != null) {
            val date = Timestamp.from(Instant.parse(v.asString))
            cal?.time = date
            if (cal == null) {
                return Timestamp(date.time)
            }
            return Timestamp(cal.time.time)
        }
        return null
    }

    override fun getAsciiStream(columnIndex: Int): InputStream {
        logger.trace { "getAsciiStream $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getAsciiStream(columnLabel: String?): InputStream {
        logger.trace { "getAsciiStream $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    @Deprecated("Deprecated in Java")
    override fun getUnicodeStream(columnIndex: Int): InputStream {
        logger.trace { "getUnicodeStream $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    @Deprecated("Deprecated in Java")
    override fun getUnicodeStream(columnLabel: String?): InputStream {
        logger.trace { "getUnicodeStream $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getBinaryStream(columnIndex: Int): InputStream {
        logger.trace { "getBinaryStream $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getBinaryStream(columnLabel: String?): InputStream {
        logger.trace { "getBinaryStream $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getWarnings(): SQLWarning? {
        logger.trace { "getWarnings  " }
        return null
    }

    override fun clearWarnings() {
        logger.trace { "clearWarnings  " }
    }

    override fun getCursorName(): String {
        logger.trace { "getCursorName  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getMetaData(): ResultSetMetaData {
        logger.trace { "getMetaData  " }
        return AdoJdbcResultSetMetaData(columns)
    }

    override fun getObject(columnIndex: Int): Any? {
        logger.trace { "getObject $columnIndex " }
        val col = columns[columnIndex - 1]
        return when (col.type.sqlType) {
            Types.NVARCHAR -> this.getString(columnIndex)
            Types.INTEGER -> this.getInt(columnIndex)
            Types.DOUBLE -> this.getDouble(columnIndex)
            Types.DATE -> this.getDate(columnIndex)
            Types.TIMESTAMP -> this.getTimestamp(columnIndex)
            Types.BOOLEAN -> this.getBoolean(columnIndex)
            else -> throw SQLException("didn't find a mapping for $col")
        }
    }

    override fun getObject(columnLabel: String?): Any? {
        logger.trace { "getObject $columnLabel " }
        val index = columnIndexesByLabel[columnLabel] ?: throw SQLException("Expected $columnLabel in row fields")
        return this.getObject(index)
    }

    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any? {
        logger.trace { "getObject $columnIndex $map" }
        return this.getObject(columnIndex)
    }

    override fun getObject(columnLabel: String?, map: MutableMap<String, Class<*>>?): Any? {
        logger.trace { "getObject $columnLabel $map" }
        val index = columnIndexesByLabel[columnLabel] ?: throw SQLException("Expected $columnLabel in row fields")
        return this.getObject(index)
    }

    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T {
        logger.trace { "getObject $columnIndex $type " }
        @Suppress("UNCHECKED_CAST")
        return this.getObject(columnIndex) as T
    }

    override fun <T : Any?> getObject(columnLabel: String?, type: Class<T>?): T {
        logger.trace { "getObject $columnLabel $type " }
        val index = columnIndexesByLabel[columnLabel] ?: throw SQLException("Expected $columnLabel in row fields")
        @Suppress("UNCHECKED_CAST")
        return this.getObject(index) as T
    }

    override fun findColumn(columnLabel: String?): Int {
        logger.trace { "findColumn $columnLabel " }
        val index = columnIndexesByLabel[columnLabel] ?: throw SQLException("Expected $columnLabel in row fields")
        return index
    }

    override fun getCharacterStream(columnIndex: Int): Reader {
        logger.trace { "getCharacterStream $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getCharacterStream(columnLabel: String?): Reader {
        logger.trace { "getCharacterStream $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun isBeforeFirst(): Boolean {
        logger.trace { "isBeforeFirst  " }
        checkClosed()
        return rowIndex < 0
    }

    override fun isAfterLast(): Boolean {
        logger.trace { "isAfterLast  " }
        checkClosed()
        return rowIndex >= rows.size
    }

    override fun isFirst(): Boolean {
        logger.trace { "isFirst  " }
        checkClosed()
        return rowIndex == 0
    }

    override fun isLast(): Boolean {
        logger.trace { "isLast  " }
        checkClosed()
        return rowIndex == rows.size
    }

    override fun beforeFirst() {
        logger.trace { "beforeFirst  " }
        checkClosed()
        rowIndex = -1
    }

    override fun afterLast() {
        logger.trace { "afterLast  " }
        checkClosed()
        rowIndex = rows.size
    }

    override fun first(): Boolean {
        logger.trace { "first  " }
        checkClosed()
        rowIndex = 0
        return rows.isNotEmpty()
    }

    override fun last(): Boolean {
        logger.trace { "last  " }
        checkClosed()
        rowIndex = rows.size - 1
        return rows.isNotEmpty()
    }

    override fun getRow(): Int {
        logger.trace { "getRow  " }
        checkClosed()
        return rowIndex + 1
    }

    override fun absolute(row: Int): Boolean {
        logger.trace { "absolute $row " }
        return false
    }

    override fun relative(rows: Int): Boolean {
        logger.trace { "relative $rows " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun previous(): Boolean {
        logger.trace { "previous  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun setFetchDirection(direction: Int) {
        logger.trace { "setFetchDirection $direction " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getFetchDirection(): Int {
        logger.trace { "getFetchDirection  " }
        checkClosed()
        return FETCH_FORWARD
    }

    override fun setFetchSize(rows: Int) {
        logger.trace { "setFetchSize $rows " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getFetchSize(): Int {
        logger.trace { "getFetchSize  " }
        checkClosed()
        return rows.size
    }

    override fun getType(): Int {
        logger.trace { "getType  " }
        checkClosed()
        return TYPE_FORWARD_ONLY
    }

    override fun getConcurrency(): Int {
        logger.trace { "getConcurrency  " }
        checkClosed()
        return CONCUR_READ_ONLY
    }

    override fun rowUpdated(): Boolean {
        logger.trace { "rowUpdated  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun rowInserted(): Boolean {
        logger.trace { "rowInserted  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun rowDeleted(): Boolean {
        logger.trace { "rowDeleted  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNull(columnIndex: Int) {
        logger.trace { "updateNull $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNull(columnLabel: String?) {
        logger.trace { "updateNull $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBoolean(columnIndex: Int, x: Boolean) {
        logger.trace { "updateBoolean $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBoolean(columnLabel: String?, x: Boolean) {
        logger.trace { "updateBoolean $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateByte(columnIndex: Int, x: Byte) {
        logger.trace { "updateByte $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateByte(columnLabel: String?, x: Byte) {
        logger.trace { "updateByte $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateShort(columnIndex: Int, x: Short) {
        logger.trace { "updateShort $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateShort(columnLabel: String?, x: Short) {
        logger.trace { "updateShort $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateInt(columnIndex: Int, x: Int) {
        logger.trace { "updateInt $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateInt(columnLabel: String?, x: Int) {
        logger.trace { "updateInt $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateLong(columnIndex: Int, x: Long) {
        logger.trace { "updateLong $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateLong(columnLabel: String?, x: Long) {
        logger.trace { "updateLong $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateFloat(columnIndex: Int, x: Float) {
        logger.trace { "updateFloat $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateFloat(columnLabel: String?, x: Float) {
        logger.trace { "updateFloat $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateDouble(columnIndex: Int, x: Double) {
        logger.trace { "updateDouble $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateDouble(columnLabel: String?, x: Double) {
        logger.trace { "updateDouble $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) {
        logger.trace { "updateBigDecimal $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBigDecimal(columnLabel: String?, x: BigDecimal?) {
        logger.trace { "updateBigDecimal $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateString(columnIndex: Int, x: String?) {
        logger.trace { "updateString $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateString(columnLabel: String?, x: String?) {
        logger.trace { "updateString $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBytes(columnIndex: Int, x: ByteArray?) {
        logger.trace { "updateBytes $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBytes(columnLabel: String?, x: ByteArray?) {
        logger.trace { "updateBytes $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateDate(columnIndex: Int, x: Date?) {
        logger.trace { "updateDate $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateDate(columnLabel: String?, x: Date?) {
        logger.trace { "updateDate $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateTime(columnIndex: Int, x: Time?) {
        logger.trace { "updateTime $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateTime(columnLabel: String?, x: Time?) {
        logger.trace { "updateTime $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateTimestamp(columnIndex: Int, x: Timestamp?) {
        logger.trace { "updateTimestamp $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateTimestamp(columnLabel: String?, x: Timestamp?) {
        logger.trace { "updateTimestamp $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Int) {
        logger.trace { "updateAsciiStream $columnIndex $x $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Int) {
        logger.trace { "updateAsciiStream $columnLabel $x $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) {
        logger.trace { "updateAsciiStream $columnIndex $x $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) {
        logger.trace { "updateAsciiStream $columnLabel $x $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {
        logger.trace { "updateAsciiStream $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateAsciiStream(columnLabel: String?, x: InputStream?) {
        logger.trace { "updateAsciiStream $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Int) {
        logger.trace { "updateBinaryStream $columnIndex $x $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Int) {
        logger.trace { "updateBinaryStream $columnLabel $x $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {
        logger.trace { "updateBinaryStream $columnIndex $x $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) {
        logger.trace { "updateBinaryStream $columnLabel $x $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {
        logger.trace { "updateBinaryStream $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBinaryStream(columnLabel: String?, x: InputStream?) {
        logger.trace { "updateBinaryStream $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {
        logger.trace { "updateCharacterStream $columnIndex $x $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Int) {
        logger.trace { "updateCharacterStream $columnLabel $reader $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        logger.trace { "updateCharacterStream $columnIndex $x $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        logger.trace { "updateCharacterStream $columnLabel $reader $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateCharacterStream(columnIndex: Int, x: Reader?) {
        logger.trace { "updateCharacterStream $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateCharacterStream(columnLabel: String?, reader: Reader?) {
        logger.trace { "updateCharacterStream $columnLabel $reader " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) {
        logger.trace { "updateObject $columnIndex $x $scaleOrLength " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateObject(columnIndex: Int, x: Any?) {
        logger.trace { "updateObject $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateObject(columnLabel: String?, x: Any?, scaleOrLength: Int) {
        logger.trace { "updateObject $columnLabel $x $scaleOrLength " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateObject(columnLabel: String?, x: Any?) {
        logger.trace { "updateObject $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun insertRow() {
        logger.trace { "insertRow  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateRow() {
        logger.trace { "updateRow  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun deleteRow() {
        logger.trace { "deleteRow  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun refreshRow() {
        logger.trace { "refreshRow  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun cancelRowUpdates() {
        logger.trace { "cancelRowUpdates  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun moveToInsertRow() {
        logger.trace { "moveToInsertRow  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun moveToCurrentRow() {
        logger.trace { "moveToCurrentRow  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getStatement(): Statement {
        logger.trace { "getStatement  " }
        checkClosed()
        return this.statement
    }

    override fun getRef(columnIndex: Int): Ref {
        logger.trace { "getRef $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getRef(columnLabel: String?): Ref {
        logger.trace { "getRef $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getBlob(columnIndex: Int): Blob {
        logger.trace { "getBlob $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getBlob(columnLabel: String?): Blob {
        logger.trace { "getBlob $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getClob(columnIndex: Int): Clob {
        logger.trace { "getClob $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getClob(columnLabel: String?): Clob {
        logger.trace { "getClob $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getArray(columnIndex: Int): Array {
        logger.trace { "getArray $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getArray(columnLabel: String?): Array {
        logger.trace { "getArray $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getURL(columnIndex: Int): URL {
        logger.trace { "getURL $columnIndex " }
        checkClosed()
        val row = rows[rowIndex]
        val col = columns[columnIndex - 1]
        val v = row[col.name] ?: throw SQLException("Expected $col in row fields")
        try {
            return URL(v.asString)
        } catch (e: MalformedURLException) {
            throw SQLException(e)
        }
    }

    override fun getURL(columnLabel: String?): URL {
        logger.trace { "getURL $columnLabel " }
        checkClosed()
        val row = rows[rowIndex]
        val v = row[columnLabel] ?: throw SQLException("Expected $columnLabel in row fields")
        try {
            return URL(v.asString)
        } catch (e: MalformedURLException) {
            throw SQLException(e)
        }
    }

    override fun updateRef(columnIndex: Int, x: Ref?) {
        logger.trace { "updateRef $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateRef(columnLabel: String?, x: Ref?) {
        logger.trace { "updateRef $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBlob(columnIndex: Int, x: Blob?) {
        logger.trace { "updateBlob $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBlob(columnLabel: String?, x: Blob?) {
        logger.trace { "updateBlob $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) {
        logger.trace { "updateBlob $columnIndex $inputStream $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?, length: Long) {
        logger.trace { "updateBlob $columnLabel $inputStream $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) {
        logger.trace { "updateBlob $columnIndex $inputStream " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateBlob(columnLabel: String?, inputStream: InputStream?) {
        logger.trace { "updateBlob $columnLabel $inputStream " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateClob(columnIndex: Int, x: Clob?) {
        logger.trace { "updateClob $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateClob(columnLabel: String?, x: Clob?) {
        logger.trace { "updateClob $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) {
        logger.trace { "updateClob $columnIndex $reader $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) {
        logger.trace { "updateClob $columnLabel $reader $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateClob(columnIndex: Int, reader: Reader?) {
        logger.trace { "updateClob $columnIndex $reader " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateClob(columnLabel: String?, reader: Reader?) {
        logger.trace { "updateClob $columnLabel $reader " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateArray(columnIndex: Int, x: Array?) {
        logger.trace { "updateArray $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateArray(columnLabel: String?, x: Array?) {
        logger.trace { "updateArray $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getRowId(columnIndex: Int): RowId {
        logger.trace { "getRowId $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getRowId(columnLabel: String?): RowId {
        logger.trace { "getRowId $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateRowId(columnIndex: Int, x: RowId?) {
        logger.trace { "updateRowId $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateRowId(columnLabel: String?, x: RowId?) {
        logger.trace { "updateRowId $columnLabel $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getHoldability(): Int {
        logger.trace { "getHoldability  " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun isClosed(): Boolean {
        logger.trace { "isClosed  " }
        return this.isClosed
    }

    override fun updateNString(columnIndex: Int, nString: String?) {
        logger.trace { "updateNString $columnIndex $nString " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNString(columnLabel: String?, nString: String?) {
        logger.trace { "updateNString $columnLabel $nString " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNClob(columnIndex: Int, nClob: NClob?) {
        logger.trace { "updateNClob $columnIndex $nClob " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNClob(columnLabel: String?, nClob: NClob?) {
        logger.trace { "updateNClob $columnLabel $nClob " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) {
        logger.trace { "updateNClob $columnIndex $reader $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) {
        logger.trace { "updateNClob $columnLabel $reader $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNClob(columnIndex: Int, reader: Reader?) {
        logger.trace { "updateNClob $columnIndex $reader " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNClob(columnLabel: String?, reader: Reader?) {
        logger.trace { "updateNClob $columnLabel $reader " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getNClob(columnIndex: Int): NClob {
        logger.trace { "getNClob $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getNClob(columnLabel: String?): NClob {
        logger.trace { "getNClob $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getSQLXML(columnIndex: Int): SQLXML {
        logger.trace { "getSQLXML $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getSQLXML(columnLabel: String?): SQLXML {
        logger.trace { "getSQLXML $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) {
        logger.trace { "updateSQLXML $columnIndex $xmlObject " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateSQLXML(columnLabel: String?, xmlObject: SQLXML?) {
        logger.trace { "updateSQLXML $columnLabel $xmlObject " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getNString(columnIndex: Int): String {
        logger.trace { "getNString $columnIndex " }
        checkClosed()
        return this.getString(columnIndex)
    }

    override fun getNString(columnLabel: String?): String {
        logger.trace { "getNString $columnLabel " }
        checkClosed()
        return this.getString(columnLabel)
    }

    override fun getNCharacterStream(columnIndex: Int): Reader {
        logger.trace { "getNCharacterStream $columnIndex " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun getNCharacterStream(columnLabel: String?): Reader {
        logger.trace { "getNCharacterStream $columnLabel " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) {
        logger.trace { "updateNCharacterStream $columnIndex $x $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?, length: Long) {
        logger.trace { "updateNCharacterStream $columnLabel $reader $length " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNCharacterStream(columnIndex: Int, x: Reader?) {
        logger.trace { "updateNCharacterStream $columnIndex $x " }
        throw SQLFeatureNotSupportedException("not implemented")
    }

    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?) {
        logger.trace { "updateNCharacterStream $columnLabel $reader " }
        throw SQLFeatureNotSupportedException("not implemented")
    }
}

