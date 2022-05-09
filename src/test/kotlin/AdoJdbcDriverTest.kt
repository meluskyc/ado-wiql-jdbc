import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import java.sql.DriverPropertyInfo
import java.util.*
import kotlin.test.assertContains
import kotlin.test.asserter

internal class AdoJdbcDriverTest {
    private val subject = AdoJdbcDriver()

    private fun testProperties(): Properties {
        val properties = Properties()
        properties.setProperty("API_URL_BASE", "https://domain.com")
        properties.setProperty("PAT", "abcd123")
        properties.setProperty("USERNAME", "userNameee")
        properties.setProperty("TEAM", "theTeam")
        properties.setProperty("PROJECT", "theProject")
        return properties
    }

    @Test
    fun connect_simple() {
        val properties = testProperties()
        val connection = subject.connect(null, properties)
        assertNotNull(connection)
    }

    @Test
    fun acceptsURL_true() {
        assertTrue(subject.acceptsURL(null))
        assertTrue(subject.acceptsURL("???"))
        assertTrue(subject.acceptsURL("https://domain.com"))
        assertTrue(subject.acceptsURL("jdbc://localhost"))
    }

    @Test
    fun getPropertyInfo_simple() {
        val driverProperties = subject
            .getPropertyInfo(null, Properties())
            .associateBy { p -> p.name }

        assertContains(driverProperties, "API_URL_BASE")
        assertContains(driverProperties, "PAT")
        assertContains(driverProperties, "USERNAME")
        assertContains(driverProperties, "TEAM")
        assertContains(driverProperties, "PROJECT")
    }
}
