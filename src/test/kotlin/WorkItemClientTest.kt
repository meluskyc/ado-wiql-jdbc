import com.google.gson.Gson
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.io.File

class WorkItemClientTest {

    @Test
    fun serializeThing() {
        val configJson: String = File("./src/test/resources/sample-response.json").readText(Charsets.UTF_8)
    }

    @Test
    fun send_Simple() = runBlocking {
        val configJson: String = File("./src/test/resources/test-credentials.json").readText(Charsets.UTF_8)
        val config = Gson().fromJson(configJson, AdoJdbcConfiguration::class.java)
        val client = WorkItemClient(config)
        val response = client.getWorkItems(listOf(15), listOf("Microsoft.VSTS.CMMI.TargetResolveDate"), null)
//        val response = client.executeWiql(
//            """
//                SELECT *
//                    FROM workitems
//                    WHERE
//                    [System.TeamProject] = @project
//            ORDER BY [System.ChangedDate] DESC
//            """
//        )
    }

    @Test
    fun getFields_Simple() = runBlocking {
        val configJson: String = File("./src/test/resources/test-credentials.json").readText(Charsets.UTF_8)
        val config = Gson().fromJson(configJson, AdoJdbcConfiguration::class.java)
        val client = WorkItemClient(config)
        val response = client.getFields()
    }
}
