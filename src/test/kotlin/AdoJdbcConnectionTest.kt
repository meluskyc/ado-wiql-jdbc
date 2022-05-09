import com.google.gson.JsonPrimitive
import mu.KotlinLogging
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import java.sql.SQLException
import java.sql.Types
import java.util.*

internal class AdoJdbcConnectionTest {
    @Test
    fun typezzz() {
        val sqlType = Types.INTEGER
        val javaType = when (sqlType) {
            Types.NVARCHAR -> String::class.java.name
            Types.INTEGER -> Int::class.java.name
            Types.DOUBLE -> Double::class.java.name
            Types.DATE -> Date::class.java.name
            Types.BOOLEAN -> Boolean::class.java.name
            else -> throw SQLException("didn't find a mapping for $sqlType")
        }
        System.out.println(javaType)
    }
}
