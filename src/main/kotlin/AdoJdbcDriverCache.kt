import mu.KotlinLogging

class AdoJdbcDriverCache {
    private val logger = KotlinLogging.logger {}
    private val cache = SimpleCache()

    object Keys {
        const val FIELDS = "fields"
        const val FIELDS_MAP = "fields_map"
    }

    fun getOrSetFields(getFieldsFactory: () -> AdoWiqlClientListFieldsResponse): AdoWiqlClientListFieldsResponse {
        logger.trace { "getOrSetFields " }
        return cache.getOrSet(Keys.FIELDS, getFieldsFactory)
    }

    fun getOrSetFieldsMap(getFieldsMapFactory: () -> Map<String, ColumnInfo>): Map<String, ColumnInfo> {
        logger.trace { "getOrSetFieldsMap " }
        return cache.getOrSet(Keys.FIELDS_MAP, getFieldsMapFactory)
    }
}
