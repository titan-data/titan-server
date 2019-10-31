package io.titandata.metadata

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.metadata.table.Repositories
import io.titandata.models.Repository
import io.titandata.serialization.ModelTypeAdapters
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update

/*
 * The metadata provider is responsible for persistence of all metadata to the titan database. With the exception of
 * init(), it's up to the caller to manage transactions.
 */
class MetadataProvider(val inMemory: Boolean = true, val databaseName:String = "titan") {

    internal val gson = ModelTypeAdapters.configure(GsonBuilder()).create()

    private fun memoryConfig(): HikariDataSource {
        val config = HikariConfig()
        config.driverClassName = "org.h2.Driver"
        config.jdbcUrl = "jdbc:h2:mem:$databaseName"
        config.maximumPoolSize = 3
        config.isAutoCommit = false
        config.transactionIsolation = "TRANSACTION_READ_COMMITTED"
        config.validate()
        return HikariDataSource(config)
    }

    private fun persistentConfig(): HikariDataSource {
        val config = HikariConfig()
        config.driverClassName = "org.postgresql.Driver"
        config.jdbcUrl = "jdbc:postgresql:$databaseName"
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
            SchemaUtils.createMissingTablesAndColumns(Repositories)
        }
    }

    private fun convertRepository(it: ResultRow) = Repository(
            name = it[Repositories.name],
            properties = gson.fromJson(it[Repositories.metadata], object : TypeToken<Map<String, Any>>() {}.type)
    )

    fun createRepository(repo: Repository) {
        try {
            Repositories.insert {
                it[name] = repo.name
                it[metadata] = gson.toJson(repo.properties)
            }
        } catch (e: ExposedSQLException) {
            throw ObjectExistsException("repository '${repo.name}' already exists")
        }
    }

    fun listRepositories(): List<Repository> {
        return Repositories.selectAll().map { convertRepository(it) }
    }

    fun getRepository(repoName: String): Repository {
        return Repositories.select {
            Repositories.name eq repoName
        }.map { convertRepository(it) }
                .firstOrNull()
                ?: throw NoSuchObjectException("no such repository '$repoName'")
    }

    fun updateRepository(repoName: String, repo: Repository) {
        try {
            val count = Repositories.update({ Repositories.name eq repoName }) {
                it[name] = repo.name
                it[metadata] = gson.toJson(repo.properties)
            }
            if (count == 0) {
                throw NoSuchObjectException("no such repository '$repoName'")
            }
        } catch (e:ExposedSQLException) {
            throw ObjectExistsException("repository '${repo.name}' already exists")
        }
    }

    fun deleteRepository(repoName: String) {
        val count = Repositories.deleteWhere {
            Repositories.name eq repoName
        }
        if (count == 0) {
            throw NoSuchObjectException("no such repository '$repoName'")
        }
    }
}
