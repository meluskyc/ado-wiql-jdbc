package adoApi

data class ListFieldsResponse(
    val count: Int,
    val value: List<Field>,
)

data class Field(
    val name: String,
    val description: String?,
    val referenceName: String,
    val type: String,
    val readOnly: Boolean,
    val supportedOperations: List<SupportedOperation>,
    val url: String,
    val isIdentity: Boolean?,
    val isQueryable: Boolean?,
    val usage: String,
)

data class SupportedOperation(
    val referenceName: String,
    val name: String,
)
