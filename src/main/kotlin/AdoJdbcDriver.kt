import mu.KotlinLogging
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverPropertyInfo
import java.sql.SQLFeatureNotSupportedException
import java.util.*
import java.util.logging.Logger

class AdoJdbcDriver : Driver {
    private val logger = KotlinLogging.logger {}

    override fun connect(_url: String?, info: Properties?): Connection {
        logger.trace { "connect $_url $info " }
        if (info == null) throw IllegalArgumentException("Properties was null")

        val config = AdoJdbcConfiguration.fromProperties(info)
        return AdoJdbcConnection(config)
    }

    override fun acceptsURL(url: String?): Boolean {
        logger.trace { "acceptsURL $url " }
        return true
    }

    override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> {
        logger.trace { "getPropertyInfo $url $info " }
        return arrayOf(
            DriverPropertyInfo(Constants.PROPERTY_API_URL_BASE, null),
            DriverPropertyInfo(Constants.PROPERTY_PROJECT, null),
            DriverPropertyInfo(Constants.PROPERTY_TEAM, null),
            DriverPropertyInfo(Constants.PROPERTY_USER_NAME, null),
            DriverPropertyInfo(Constants.PROPERTY_PAT, null),
        )
    }

    override fun getMajorVersion(): Int {
        logger.trace { "getMajorVersion  " }
        return Constants.DRIVER_VERSION_MAJOR
    }

    override fun getMinorVersion(): Int {
        logger.trace { "getMinorVersion  " }
        return Constants.DRIVER_VERSION_MINOR
    }

    override fun jdbcCompliant(): Boolean {
        logger.trace { "jdbcCompliant  " }
        return false
    }

    override fun getParentLogger(): Logger {
        logger.trace { "getParentLogger  " }
        throw SQLFeatureNotSupportedException("logger not supported")
    }
}
