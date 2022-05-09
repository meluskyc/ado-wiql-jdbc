package adoApi

data class GetWorkItemsBatchRequest(
    val ids: List<Long>,
    val fields: List<String>
)