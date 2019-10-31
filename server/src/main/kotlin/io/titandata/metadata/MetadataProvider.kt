package io.titandata.metadata

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import io.titandata.metadata.table.Repositories

/*
 * The metadata provider is responsible for persistence of all metadata to the titan database.
 */
class MetadataProvider(val inMemory:Boolean = true) {

    private fun memoryConfig() : HikariDataSource {
        val config = HikariConfig()
        config.driverClassName = "org.h2.Driver"
        config.jdbcUrl = "jdbc:h2:mem:titan"
        config.maximumPoolSize = 3
        config.isAutoCommit = false
        config.transactionIsolation = "TRANSACTION_READ_COMMITTED"
        config.validate()
        return HikariDataSource(config)
    }

    private fun persistentConfig() : HikariDataSource {
        val config = HikariConfig()
        config.driverClassName = "org.postgresql.Driver"
        config.jdbcUrl = "jdbc:postgresql:titan"
        config.username = "postgres"
        config.password = "postgres"
        config.maximumPoolSize = 3
        config.isAutoCommit = false
        config.transactionIsolation = "TRANSACTION_READ_COMMITTED"
        config.validate()
        return HikariDataSource(config)
    }

    fun init() {
        if (inMemory) {
            Database.connect(memoryConfig())
        } else {
            Database.connect(persistentConfig())
        }

        transaction {
            SchemaUtils.createMissingTablesAndColumns(Repositories())
        }
    }
}