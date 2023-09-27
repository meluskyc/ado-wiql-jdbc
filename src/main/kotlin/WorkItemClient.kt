import adoApi.*
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.apache.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.gson.*
import mu.KotlinLogging
import org.apache.http.conn.ssl.TrustAllStrategy
import org.apache.http.ssl.SSLContextBuilder
import java.net.URL
import java.sql.SQLException
import java.util.*

data class AdoWiqlClientSendResult(
    val wiqlResponse: WiqlResponse,
    val workItemsResponse: GetWorkItemsBatchResponse,
)

data class AdoWiqlClientListFieldsResponse(
    val listFieldsResponse: ListFieldsResponse,
)

fun AdoWiqlClientListFieldsResponse.mapFieldsToColumnInfo(): Map<String, ColumnInfo> {
    return this.listFieldsResponse.value
        .map { f -> Pair(f, Constants.adoFieldToType[f.type]) }
        .filter { (_, typeInfo) -> typeInfo != null }
        .map { (f, typeInfo) ->
            typeInfo ?: throw SQLException("typeInfo for ${f.type} was null")
            ColumnInfo(
                Constants.adoUsageToTableName[f.usage] ?: throw SQLException("Unexpected usage table ${f.usage}"),
                f.referenceName,
                typeInfo,
                typeInfo.maxSize,
                f.isQueryable ?: false,
                f.isIdentity ?: false,
            )
        }.associateBy { columnInfo: ColumnInfo -> columnInfo.name }
}

class WorkItemClient(val config: AdoJdbcConfiguration) {
    private val userAgent = "jdbc-wiql"
    private val logger = KotlinLogging.logger {}

    private val client = HttpClient(Apache) {
        expectSuccess = true
        install(ContentNegotiation) {
            gson()
        }
        engine {
            customizeClient {
                setSSLContext(
                    SSLContextBuilder.create().loadTrustMaterial(TrustAllStrategy()).build()
                )
            }
        }
    }

    suspend fun executeWiql(wiql: String, top: Int = 2000): WiqlResponse {
        logger.trace { "executeWiql $wiql $top " }

        val baseUrl = config.adoApiUrlBase
        val wiqlRequestBuilder = HttpRequestBuilder()
        wiqlRequestBuilder.url {
            protocol = URLProtocol(baseUrl.protocol, baseUrl.defaultPort)
            host = baseUrl.host
            appendPathSegments(
                listOf(
                    baseUrl.path, config.adoProject, config.adoTeam, "_apis", "wit", "wiql"
                )
            )
        }
        if (baseUrl.port > 0) {
            wiqlRequestBuilder.port = baseUrl.port
        }
        wiqlRequestBuilder.method = HttpMethod.Get
        val request = wiqlRequestBuilder.build()
        val wiqlResponse = client.post(request.url) {
            contentType(ContentType.Application.Json)
            setBody(WiqlRequest(wiql))
            headers {
                addHeaders(this)
            }
            parameter("top", top)
            parameter("timePrecision", true)
        }.body<WiqlResponse>()

        return wiqlResponse
    }

    suspend fun getWorkItems(ids: List<Long>, fields: List<String>, asOf: Date?): GetWorkItemsBatchResponse {
        logger.trace { "getWorkItems $ids $fields $asOf " }
        val workItemsUrl = URL("${config.adoApiUrlBase}/${config.adoProject}/_apis/wit/workitemsbatch")
        return client.post(workItemsUrl) {
            headers {
                addHeaders(this)
            }
//            parameter("asOf", asOf.toInstant())
            contentType(ContentType.Application.Json)
            setBody(GetWorkItemsBatchRequest(ids, fields))
        }.body()
    }

    private fun addHeaders(headersBuilder: HeadersBuilder): HeadersBuilder {
        val userNameAndPat = "${config.adoUserName}:${config.adoPersonalAccessToken}"
        val encodedUserNameAndPat: String = Base64.getEncoder().encodeToString(userNameAndPat.toByteArray())
        headersBuilder.append(HttpHeaders.Authorization, "Basic $encodedUserNameAndPat")
        headersBuilder.append(HttpHeaders.UserAgent, userAgent)
        headersBuilder.append(HttpHeaders.Accept, "application/json;api-version=6.0;excludeUrls=true")
        return headersBuilder
    }

    suspend fun getFields(): AdoWiqlClientListFieldsResponse {
        logger.trace { "getFields  " }
        val fieldsUrl = URL("${config.adoApiUrlBase}/_apis/wit/fields")
        val fieldsResponse = client.get(fieldsUrl) {
            contentType(ContentType.Application.Json)
            headers {
                addHeaders(this)
            }
        }.body<ListFieldsResponse>()

        return AdoWiqlClientListFieldsResponse(fieldsResponse)
    }
}
