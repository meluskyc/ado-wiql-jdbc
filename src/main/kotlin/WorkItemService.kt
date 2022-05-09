import adoApi.WorkItem
import adoApi.GetWorkItemsBatchResponse
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class WorkItemService(config: AdoJdbcConfiguration) {
    private val client = WorkItemClient(config)
    private val logger = KotlinLogging.logger {}

    private val cache: AdoJdbcDriverCache = AdoJdbcDriverCache()

    fun executeWiql(wiql: String): AdoWiqlClientSendResult {
        logger.trace { "executeWiql $wiql " }
        return runBlocking {
            val fields = cache.getOrSetFieldsMap {
                runBlocking {
                    client.getFields().mapFieldsToColumnInfo()
                }
            }

            val wiqlResponse = client.executeWiql(wiql)

            if (wiqlResponse.workItems.isEmpty() || wiqlResponse.columns.isEmpty()) {
                return@runBlocking AdoWiqlClientSendResult(wiqlResponse, GetWorkItemsBatchResponse(0, arrayListOf()))
            }

            val ids = wiqlResponse.workItems.map { wi -> wi.id }
            val columns = wiqlResponse.columns
                .filter { c -> fields[c.referenceName]?.isQueryable == true }
                .map { c -> c.referenceName }

            val workItemsById = mutableMapOf<Int, WorkItem>()
            for (batch in ids.chunked(200)) {
                val workItemsResponse = client.getWorkItems(batch, columns, wiqlResponse.asOf)
                for (row in workItemsResponse.value) {
                    if (workItemsById.containsKey(row.id)) {
                        for (key in row.fields.keys) {
                            workItemsById[row.id]!!.fields[key] = row.fields[key]!!
                        }
                    } else {
                        workItemsById[row.id] = row
                    }
                }
            }

            return@runBlocking AdoWiqlClientSendResult(
                wiqlResponse,
                GetWorkItemsBatchResponse(wiqlResponse.workItems.size, workItemsById.values.toList())
            )
        }
    }

    fun getFields(skipCache: Boolean = false): AdoWiqlClientListFieldsResponse {
        logger.trace { "getField $skipCache " }
        return if (skipCache) {
            runBlocking {
                client.getFields()
            }
        } else {
            cache.getOrSetFields {
                runBlocking {
                    client.getFields()
                }
            }
        }
    }

    fun getFieldsMap(skipCache: Boolean = false): Map<String, ColumnInfo> {
        logger.trace { "getFieldsMap $skipCache " }
        return if (skipCache) {
            runBlocking {
                client.getFields().mapFieldsToColumnInfo()
            }
        } else {
            cache.getOrSetFieldsMap {
                runBlocking {
                    client.getFields().mapFieldsToColumnInfo()
                }
            }
        }
    }
}
