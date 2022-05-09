package adoApi

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.stream.JsonToken

data class GetWorkItemsBatchResponse(
    val count: Int,
    val value: List<WorkItem>,
)

data class WorkItem(
    val id: Int,
    val rev: Int,
    val fields: MutableMap<String, JsonElement>,
    val url: String,
)

data class WorkItemIdentity(
    val displayName: String,
    val url: String,
    val _links: Map<String, JsonToken>,
    val id: String,
    val uniqueName: String,
    val imageUrl: String,
    val descriptor: String,
)

fun JsonObject.displayIdentityColumn(): String {
    val displayName = this["displayName"].asString
    val uniqueName = this["uniqueName"].asString
    return "$displayName <$uniqueName>"
}
