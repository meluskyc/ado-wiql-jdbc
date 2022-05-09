package adoApi

import java.util.*

data class WiqlResponse(
    val queryType: String,
    val asOf: Date,
    val columns: List<WiqlResponseColumn>,
    val sortColumns: List<WiqlResponseSortColumn>,
    val workItems: List<WiqlResponseWorkItem>
)

data class WiqlResponseColumn(
    val referenceName: String,
    val name: String,
)

data class WiqlResponseSortColumn(
    val field: WiqlResponseColumn,
    val descending: Boolean
)

data class WiqlResponseWorkItem(
    val id: Long,
)
