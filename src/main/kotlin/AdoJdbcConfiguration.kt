import mu.KotlinLogging
import java.net.URL
import java.util.*

class AdoJdbcConfiguration(
    val adoApiUrlBase: URL,
    val adoProject: String,
    val adoTeam: String,
    val adoUserName: String,
    val adoPersonalAccessToken: String,
) {
    companion object Factory {
        private val logger = KotlinLogging.logger {}
        fun acceptsURL(url: String?): Boolean {
            logger.trace { "acceptsURL $url " }
            return true
        }

        fun fromProperties(properties: Properties): AdoJdbcConfiguration {
            logger.trace { "init $properties " }
            val urlString = properties.getProperty(Constants.PROPERTY_API_URL_BASE)
            val personalAccessToken = properties.getProperty(Constants.PROPERTY_PAT)
            val userName = properties.getProperty(Constants.PROPERTY_USER_NAME)
            val team = properties.getProperty(Constants.PROPERTY_TEAM)
            val project = properties.getProperty(Constants.PROPERTY_PROJECT)
            val adoUrl = URL(urlString)
            return AdoJdbcConfiguration(adoUrl, project, team, userName, personalAccessToken)
        }
    }
}
