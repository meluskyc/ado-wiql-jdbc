import mu.KotlinLogging
import java.sql.SQLException

class SimpleCache {
    private val lock = Any()
    private val cache = mutableMapOf<String, Any>()
    private val logger = KotlinLogging.logger {}


    fun <T> get(key: String): T? {
        logger.trace { "get $key " }
        synchronized(lock) {
            @Suppress("UNCHECKED_CAST") return cache[key] as T?
        }
    }

    fun <T> set(key: String, v: T) {
        logger.trace { "set $key $v " }
        synchronized(lock) {
            cache[key] = v as Any
        }
    }

    fun <T> getOrSet(key: String, orElse: () -> T): T {
        logger.trace { "getOrSet $key $orElse " }
        val v = get(key) ?: orElse()
        set(key, v)
        return v ?: throw SQLException("Failed to set key $key")
    }
}
