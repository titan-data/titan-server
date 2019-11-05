package io.titandata.metadata

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.metadata.table.Remotes
import io.titandata.metadata.table.Repositories
import io.titandata.metadata.table.VolumeSets
import io.titandata.metadata.table.Volumes
import io.titandata.models.Remote
import io.titandata.models.Repository
import io.titandata.models.Volume
import io.titandata.serialization.ModelTypeAdapters
import java.util.UUID
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.deleteAll
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
class MetadataProvider(val inMemory: Boolean = true, val databaseName: String = "titan") {

    enum class VolumeState {
        INACTIVE,
        ACTIVE,
        DELETING
    }

    private val gson = ModelTypeAdapters.configure(GsonBuilder()).create()

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
            SchemaUtils.createMissingTablesAndColumns(Repositories, Remotes, VolumeSets, Volumes)
        }
    }

    fun clear() {
        transaction {
            Volumes.deleteAll()
            VolumeSets.deleteAll()
            Repositories.deleteAll()
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
        } catch (e: ExposedSQLException) {
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

    private fun convertRemote(it: ResultRow): Remote {
        return gson.fromJson(it[Remotes.metadata], Remote::class.java)
    }

    fun addRemote(repoName: String, remote: Remote) {
        getRepository(repoName) // check existence
        try {
            Remotes.insert {
                it[name] = remote.name
                it[repo] = repoName
                it[metadata] = gson.toJson(remote)
            }
        } catch (e: ExposedSQLException) {
            throw ObjectExistsException("remote '${remote.name}' already exists in repository $repoName")
        }
    }

    fun getRemote(repoName: String, remoteName: String): Remote {
        getRepository(repoName) // check existence
        return Remotes.select {
            (Remotes.name eq remoteName) and (Remotes.repo eq repoName)
        }.map { convertRemote(it) }
                .firstOrNull()
                ?: throw NoSuchObjectException("no such remote '$remoteName' in repository '$repoName'")
    }

    fun listRemotes(repoName: String): List<Remote> {
        getRepository(repoName) // check existence
        return Remotes.select {
            Remotes.repo eq repoName
        }.map { convertRemote(it) }
    }

    fun removeRemote(repoName: String, remoteName: String) {
        getRepository(repoName) // check existence
        val count = Remotes.deleteWhere {
            (Remotes.name eq remoteName) and (Remotes.repo eq repoName)
        }
        if (count == 0) {
            throw NoSuchObjectException("no such remote '$remoteName' in repository '$repoName'")
        }
    }

    fun updateRemote(repoName: String, remoteName: String, remote: Remote) {
        getRepository(repoName) // check existence
        try {
            val count = Remotes.update({
                (Remotes.name eq remoteName) and (Remotes.repo eq repoName)
            }) {
                it[name] = remote.name
                it[metadata] = gson.toJson(remote)
            }
            if (count == 0) {
                throw NoSuchObjectException("no such remote '$remoteName' in repository '$repoName'")
            }
        } catch (e: ExposedSQLException) {
            throw ObjectExistsException("remote '${remote.name}' already exists in repository '$repoName'")
        }
    }

    fun createVolumeSet(repoName: String, activate: Boolean = false): String {
        getRepository(repoName)

        val id = VolumeSets.insert {
            it[repo] = repoName
            it[state] = if (activate) { VolumeState.ACTIVE } else { VolumeState.INACTIVE }
        } get VolumeSets.id
        return id.toString()
    }

    fun getActiveVolumeSet(repoName: String): String {
        getRepository(repoName)
        return VolumeSets.select {
            (VolumeSets.repo eq repoName) and (VolumeSets.state eq VolumeState.ACTIVE)
        }.map { it[VolumeSets.id].toString() }
                .firstOrNull()!!
    }

    fun activateVolumeSet(repoName: String, volumeSet: String) {
        getRepository(repoName)
        VolumeSets.update({
            (VolumeSets.repo eq repoName) and (VolumeSets.state eq VolumeState.ACTIVE)
        }) {
            it[state] = VolumeState.INACTIVE
        }
        val count = VolumeSets.update({
            (VolumeSets.repo eq repoName) and (VolumeSets.id eq UUID.fromString(volumeSet))
        }) {
            it[state] = VolumeState.ACTIVE
        }
        if (count == 0) {
            // This should never happen, not a user-visible exception
            throw Exception("no such volume set '$volumeSet' in repository '$repoName'")
        }
    }

    fun markVolumeSetDeleting(volumeSet: String) {
        VolumeSets.update({
            VolumeSets.id eq UUID.fromString(volumeSet)
        }) {
            it[state] = VolumeState.DELETING
        }
    }

    fun deleteVolumeSet(volumeSet: String) {
        VolumeSets.deleteWhere {
            VolumeSets.id eq UUID.fromString(volumeSet)
        }
    }

    fun listDeletingVolumeSets(): List<String> {
        return VolumeSets.select {
            VolumeSets.state eq VolumeState.DELETING
        }.map { it[VolumeSets.id].toString() }
    }

    private fun convertVolume(it: ResultRow) = Volume(
            name = it[Volumes.name],
            properties = gson.fromJson(it[Volumes.metadata], object : TypeToken<Map<String, Any>>() {}.type)
    )

    fun createVolume(volumeSet: String, volume: Volume) {
        try {
            Volumes.insert {
                it[Volumes.volumeSet] = UUID.fromString(volumeSet)
                it[name] = volume.name
                it[metadata] = gson.toJson(volume.properties)
                it[state] = VolumeState.INACTIVE
            }
        } catch (e: ExposedSQLException) {
            throw ObjectExistsException("volume '${volume.name}' already exists")
        }
    }

    fun markVolumeDeleting(volumeSet: String, volumeName: String) {
        val count = Volumes.update({
            (Volumes.volumeSet eq UUID.fromString(volumeSet)) and (Volumes.name eq volumeName)
        }) {
            it[state] = VolumeState.DELETING
        }
        if (count == 0) {
            throw NoSuchObjectException("no such volume '$volumeName'")
        }
    }

    fun deleteVolume(volumeSet: String, volumeName: String) {
        val count = Volumes.deleteWhere {
            (Volumes.volumeSet eq UUID.fromString(volumeSet)) and (Volumes.name eq volumeName)
        }
        if (count == 0) {
            throw NoSuchObjectException("no such volume '$volumeName'")
        }
    }

    fun getVolume(volumeSet: String, volumeName: String): Volume {
        return Volumes.select {
            (Volumes.volumeSet eq UUID.fromString(volumeSet)) and (Volumes.name eq volumeName)
        }.map { convertVolume(it) }
                .firstOrNull()
                ?: throw NoSuchObjectException("no such volume '$volumeName'")
    }

    fun listVolumes(volumeSet: String): List<Volume> {
        return Volumes.select {
            Volumes.volumeSet eq UUID.fromString(volumeSet)
        }.map { convertVolume(it) }
    }

    fun listAllVolumes(): List<Volume> {
        return Volumes.selectAll().map { convertVolume(it) }
    }
}
