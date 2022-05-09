import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.net.MalformedURLException
import java.util.*
import kotlin.test.assertFailsWith

internal class AdoJdbcConfigurationTest {

    @Test
    fun fromProperties_badUrl() {
        val properties = Properties()
        properties.setProperty("API_URL_BASE", "bad.url")

        assertFailsWith<MalformedURLException>(
            message = "no protocol: bad.url",
            block = {
                AdoJdbcConfiguration.fromProperties(properties)
            }
        )
    }

    @Test
    fun fromProperties_simple() {
        val properties = Properties()
        properties.setProperty("API_URL_BASE", "https://domain.com")
        properties.setProperty("PAT", "abcd123")
        properties.setProperty("USERNAME", "userName992")
        properties.setProperty("TEAM", "teammmmm")
        properties.setProperty("PROJECT", "theProject")

        val config = AdoJdbcConfiguration.fromProperties(properties)
        assertEquals("https://domain.com", config.adoApiUrlBase.toString())
        assertEquals("abcd123", config.adoPersonalAccessToken)
        assertEquals("userName992", config.adoUserName)
        assertEquals("teammmmm", config.adoTeam)
        assertEquals("theProject", config.adoProject)
    }
}
