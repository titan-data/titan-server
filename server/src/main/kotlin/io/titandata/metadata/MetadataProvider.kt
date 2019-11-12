package io.titandata.metadata

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.metadata.table.Commits
import io.titandata.metadata.table.Operations
import io.titandata.metadata.table.ProgressEntries
import io.titandata.metadata.table.Remotes
import io.titandata.metadata.table.Repositories
import io.titandata.metadata.table.Tags
import io.titandata.metadata.table.VolumeSets
import io.titandata.metadata.table.Volumes
import io.titandata.models.Commit
import io.titandata.models.Operation
import io.titandata.models.ProgressEntry
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import io.titandata.models.Repository
import io.titandata.models.docker.DockerVolume
import io.titandata.serialization.ModelTypeAdapters
import io.titandata.storage.OperationData
import java.util.UUID
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.Op
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.andWhere
import org.jetbrains.exposed.sql.compoundOr
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import org.joda.time.DateTime

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
            SchemaUtils.createMissingTablesAndColumns(Repositories, Remotes, VolumeSets, Volumes, Commits, Tags,
                    Operations, ProgressEntries)
        }
    }

    fun clear() {
        transaction {
            Operations.deleteAll()
            Commits.deleteAll()
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
        return Remotes.select {
            (Remotes.name eq remoteName) and (Remotes.repo eq repoName)
        }.map { convertRemote(it) }
                .firstOrNull()
                ?: throw NoSuchObjectException("no such remote '$remoteName' in repository '$repoName'")
    }

    fun listRemotes(repoName: String): List<Remote> {
        return Remotes.select {
            Remotes.repo eq repoName
        }.map { convertRemote(it) }
    }

    fun removeRemote(repoName: String, remoteName: String) {
        val count = Remotes.deleteWhere {
            (Remotes.name eq remoteName) and (Remotes.repo eq repoName)
        }
        if (count == 0) {
            throw NoSuchObjectException("no such remote '$remoteName' in repository '$repoName'")
        }
    }

    fun updateRemote(repoName: String, remoteName: String, remote: Remote) {
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

    fun createVolumeSet(repoName: String, sourceCommit: String? = null, activate: Boolean = false): String {
        val sourceId = if (sourceCommit == null) {
            null
        } else {
            Commits.select {
                (Commits.repo eq repoName) and (Commits.guid eq sourceCommit) and (Commits.state eq VolumeState.ACTIVE)
            }.map {
                it[Commits.id].value
            }.firstOrNull()
        }

        val id = VolumeSets.insert {
            it[repo] = repoName
            it[VolumeSets.sourceCommit] = sourceCommit
            it[VolumeSets.sourceId] = sourceId
            it[state] = if (activate) { VolumeState.ACTIVE } else { VolumeState.INACTIVE }
        } get VolumeSets.id
        return id.toString()
    }

    fun getActiveVolumeSet(repoName: String): String {
        return VolumeSets.select {
            (VolumeSets.repo eq repoName) and (VolumeSets.state eq VolumeState.ACTIVE)
        }.map { it[VolumeSets.id].toString() }
                .firstOrNull()!!
    }

    fun getVolumeSetRepo(volumeSet: String): String? {
        val uuid = UUID.fromString(volumeSet)
        return VolumeSets.select {
            VolumeSets.id eq uuid
        }.map { it[VolumeSets.repo] }
                .firstOrNull()
    }

    fun activateVolumeSet(repoName: String, volumeSet: String) {
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
            throw IllegalArgumentException("no such volume set '$volumeSet' in repository '$repoName'")
        }
    }

    fun markVolumeSetDeleting(volumeSet: String) {
        val uuid = UUID.fromString(volumeSet)
        VolumeSets.update({
            VolumeSets.id eq uuid
        }) {
            it[state] = VolumeState.DELETING
        }
        // Mark all commits in volumeset deleting
        Commits.update({
            Commits.volumeSet eq uuid
        }) {
            it[state] = VolumeState.DELETING
        }
    }

    fun isVolumeSetEmpty(volumeSet: String): Boolean {
        val uuid = UUID.fromString(volumeSet)
        return Commits.select {
            Commits.volumeSet eq uuid
        }.count() == 0
    }

    fun listInactiveVolumeSets(): List<String> {
        return VolumeSets.select {
            VolumeSets.state eq VolumeState.INACTIVE
        }.map { it[VolumeSets.id].toString() }
    }

    fun markAllVolumeSetsDeleting(repo: String) {
        var volumeSets = VolumeSets.select {
            VolumeSets.repo eq repo
        }.map { it[VolumeSets.id] }
        for (vs in volumeSets) {
            markVolumeSetDeleting(vs.toString())
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

    private fun convertVolume(it: ResultRow) = DockerVolume(
            name = it[Volumes.name],
            properties = gson.fromJson(it[Volumes.metadata], object : TypeToken<Map<String, Any>>() {}.type)
    )

    fun createVolume(volumeSet: String, volume: DockerVolume) {
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

    fun getVolume(volumeSet: String, volumeName: String): DockerVolume {
        return Volumes.select {
            (Volumes.volumeSet eq UUID.fromString(volumeSet)) and (Volumes.name eq volumeName) and (Volumes.state neq VolumeState.DELETING)
        }.map { convertVolume(it) }
                .firstOrNull()
                ?: throw NoSuchObjectException("no such volume '$volumeName'")
    }

    fun listVolumes(volumeSet: String): List<DockerVolume> {
        return Volumes.select {
            Volumes.volumeSet eq UUID.fromString(volumeSet)
        }.map { convertVolume(it) }
    }

    fun listAllVolumes(): List<DockerVolume> {
        return Volumes.selectAll().map { convertVolume(it) }
    }

    fun listDeletingVolumes(): List<Pair<String, String>> {
        return Volumes.select {
            Volumes.state eq VolumeState.DELETING
        }.map { Pair(it[Volumes.volumeSet].toString(), it[Volumes.name]) }
    }

    private fun convertCommit(it: ResultRow) = Commit(
            id = it[Commits.guid],
            properties = gson.fromJson(it[Commits.metadata], object : TypeToken<Map<String, Any>>() {}.type)
    )

    private fun getTimestamp(commit: Commit): DateTime {
        val timestampString = commit.properties.get("timestamp")
        return if (timestampString != null) {
            DateTime.parse(timestampString.toString())
        } else {
            DateTime.now()
        }
    }

    fun createCommit(repo: String, volumeSet: String, commit: Commit) {
        val id = Commits.insert {
            it[Commits.repo] = repo
            it[Commits.volumeSet] = UUID.fromString(volumeSet)
            it[sourceCommit] = getCommitSource(volumeSet)
            it[timestamp] = getTimestamp(commit)
            it[guid] = commit.id
            it[metadata] = gson.toJson(commit.properties)
            it[state] = VolumeState.ACTIVE
        } get Commits.id
        @Suppress("UNCHECKED_CAST")
        val tags = commit.properties["tags"] as Map<String, String>?
        if (tags != null) {
            for ((key, value) in tags) {
                Tags.insert {
                    it[Tags.commit] = id.value
                    it[Tags.key] = key
                    it[Tags.value] = value
                }
            }
        }
    }

    fun getCommit(repo: String, commitId: String): Pair<String, Commit> {
        return Commits.select {
            (Commits.repo eq repo) and (Commits.guid eq commitId) and (Commits.state eq VolumeState.ACTIVE)
        }.map {
            Pair(it[Commits.volumeSet].toString(), convertCommit(it))
        }.firstOrNull()
                ?: throw NoSuchObjectException("no such commit '$commitId' in repository '$repo'")
    }

    fun getLastCommit(repo: String): String? {
        return Commits.select {
            (Commits.repo eq repo) and (Commits.state eq VolumeState.ACTIVE)
        }.orderBy(Commits.timestamp, SortOrder.DESC)
                .limit(1)
                .map { it[Commits.guid] }
                .firstOrNull()
    }

    fun getCommitSource(volumeSet: String): String? {
        // First, check to see if there's a latest commit for this volume set
        val volumeSetGuid = UUID.fromString(volumeSet)
        val prevCommit = Commits.select {
            Commits.volumeSet eq volumeSetGuid
        }.orderBy(Commits.timestamp, SortOrder.DESC)
                .limit(1)
                .firstOrNull()

        if (prevCommit != null) {
            return prevCommit[Commits.guid]
        }

        // Otherwise, look at the source of the volumeset
        val volumeSetSource = VolumeSets.select {
            VolumeSets.id eq volumeSetGuid
        }.firstOrNull()

        if (volumeSetSource != null) {
            return volumeSetSource[VolumeSets.sourceCommit]
        }

        return null
    }

    fun tagsMatch(commit: Commit, existCheck: List<String>, matchCheck: Map<String, String>): Boolean {
        @Suppress("UNCHECKED_CAST")
        val tags = commit.properties["tags"] as Map<String, String>? ?: return false

        for (key in existCheck) {
            if (!tags.containsKey(key)) {
                return false
            }
        }

        for ((key, value) in matchCheck) {
            if (tags[key] != value) {
                return false
            }
        }

        return true
    }

    fun listCommits(repo: String, tags: List<String>? = null): List<Commit> {
        // Build the search criteria
        val existCheck = mutableListOf<String>()
        val matchCheck = mutableMapOf<String, String>()
        tags?.let {
            for (tag in tags) {
                if (tag.contains("=")) {
                    val key = tag.substringBefore("=")
                    val value = tag.substringAfter("=")
                    matchCheck[key] = value
                } else {
                    existCheck.add(tag)
                }
            }
        }

        val query = if (!tags.isNullOrEmpty()) {
            val q = (Commits innerJoin Tags)
                    .slice(Commits.guid, Commits.volumeSet, Commits.metadata, Commits.timestamp)
                    .select { ((Commits.id eq Tags.commit) and (Commits.repo eq repo) and (Commits.state eq VolumeState.ACTIVE)) }

            /*
             * For filtering by multiple tags, we want to avoid complex temporary tables, etc. So we instead find tags
             * that match any such filter, and then post-process the list to do the AND query. This will get more
             * complicated if we want to do pagination, etc.
             */
            val conditions = mutableListOf<Op<Boolean>>()
            if (existCheck.size != 0) {
                conditions.add(Op.build {
                    Tags.key inList existCheck
                })
            }

            for ((key, value) in matchCheck) {
                conditions.add(Op.build {
                    (Tags.key eq key) and (Tags.value eq value)
                })
            }

            q.andWhere {
                conditions.compoundOr()
            }.withDistinct(true)
            q
        } else {
            Commits.select { (Commits.repo eq repo) and (Commits.state eq VolumeState.ACTIVE) }
        }

        val result = query.orderBy(Commits.timestamp, SortOrder.DESC)
                .map { convertCommit(it) }

        if (tags.isNullOrEmpty()) {
            return result
        } else {
            return result.filter { tagsMatch(it, existCheck, matchCheck) }
        }
    }

    data class CommitInfo(
        val id: Int,
        val guid: String,
        var volumeSet: String
    )

    fun listDeletingCommits(): List<CommitInfo> {
        return Commits.select {
            Commits.state eq VolumeState.DELETING
        }.map {
            CommitInfo(
                    id = it[Commits.id].value,
                    guid = it[Commits.guid],
                    volumeSet = it[Commits.volumeSet].toString()
            )
        }
    }

    fun hasClones(commit: CommitInfo): Boolean {
        return !VolumeSets.select {
            VolumeSets.sourceId eq commit.id
        }.empty()
    }

    fun updateCommit(repo: String, commit: Commit) {
        val id = Commits.select {
            (Commits.repo eq repo) and (Commits.guid eq commit.id) and (Commits.state eq VolumeState.ACTIVE)
        }.map { it[Commits.id].value }
                .firstOrNull()
                ?: throw NoSuchObjectException("no such commit '${commit.id}' in repository '$repo'")
        Commits.update({
            Commits.id eq id
        }) {
            it[timestamp] = getTimestamp(commit)
            it[metadata] = gson.toJson(commit.properties)
        }
        Tags.deleteWhere {
            Tags.commit eq id
        }
        @Suppress("UNCHECKED_CAST")
        val tags = commit.properties["tags"] as Map<String, String>?
        if (tags != null) {
            for ((key, value) in tags) {
                Tags.insert {
                    it[Tags.commit] = id
                    it[Tags.key] = key
                    it[Tags.value] = value
                }
            }
        }
    }

    fun markCommitDeleting(repo: String, commitId: String) {
        val count = Commits.update({
            (Commits.repo eq repo) and (Commits.guid eq commitId) and (Commits.state eq VolumeState.ACTIVE)
        }) {
            it[state] = VolumeState.DELETING
        }
        if (count == 0) {
            throw NoSuchObjectException("no such commit '$commitId' in repository '$repo'")
        }
    }

    fun deleteCommit(commit: CommitInfo) {
        Commits.deleteWhere {
            (Commits.id eq commit.id)
        }
    }

    fun createOperation(repo: String, volumeSet: String, data: OperationData) {
        Operations.insert {
            it[Operations.volumeSet] = UUID.fromString(volumeSet)
            it[Operations.repo] = repo
            it[metadataOnly] = data.metadataOnly
            it[remoteParameters] = gson.toJson(data.params)
            it[remote] = data.operation.remote
            it[commitId] = data.operation.commitId
            it[type] = data.operation.type
            it[state] = data.operation.state
        }
    }

    fun deleteOperation(volumeSet: String) {
        val uuid = UUID.fromString(volumeSet)
        val count = Operations.deleteWhere {
            Operations.volumeSet eq uuid
        }
        if (count == 0) {
            throw NoSuchObjectException("no such operation '$volumeSet")
        }
    }

    private fun convertOperation(it: ResultRow) = OperationData(
            metadataOnly = it[Operations.metadataOnly],
            params = gson.fromJson(it[Operations.remoteParameters], RemoteParameters::class.java),
            operation = Operation(
                    id = it[Operations.volumeSet].toString(),
                    remote = it[Operations.remote],
                    commitId = it[Operations.commitId],
                    type = it[Operations.type],
                    state = it[Operations.state]
            )
    )

    fun listOperations(repo: String): List<OperationData> {
        return Operations.select {
            Operations.repo eq repo
        }.map { convertOperation(it) }
    }

    fun getOperation(id: String): OperationData {
        val uuid = UUID.fromString(id)
        return Operations.select {
            Operations.volumeSet eq uuid
        }.map { convertOperation(it) }
                .firstOrNull()
                ?: throw NoSuchObjectException("no such operation '$id'")
    }

    fun operationExists(id: String): Boolean {
        val uuid = UUID.fromString(id)
        return Operations.select {
            Operations.volumeSet eq uuid
        }.count() > 0
    }

    fun updateOperationState(id: String, state: Operation.State) {
        val uuid = UUID.fromString(id)
        Operations.update({
            Operations.volumeSet eq uuid
        }) {
            it[Operations.state] = state
        }
    }

    fun operationInProgress(repo: String, type: Operation.Type, commitId: String, remote: String?): String? {
        val query = Operations.select {
            (Operations.repo eq repo) and (Operations.type eq type) and (Operations.commitId eq commitId) and (Operations.state eq Operation.State.RUNNING)
        }
        remote?.let {
            query.andWhere { Operations.remote eq remote }
        }
        return query.map { it[Operations.volumeSet].toString() }
                .firstOrNull()
    }

    private fun convertProgressEntry(it: ResultRow) = ProgressEntry(
            id = it[ProgressEntries.id].value,
            message = it[ProgressEntries.message],
            percent = it[ProgressEntries.percent],
            type = it[ProgressEntries.type]
    )

    fun addProgressEntry(operation: String, entry: ProgressEntry): Int {
        val result = ProgressEntries.insert {
            it[ProgressEntries.operation] = UUID.fromString(operation)
            it[message] = entry.message
            it[type] = entry.type
            it[percent] = entry.percent
        } get ProgressEntries.id
        return result.value
    }

    fun listProgressEntries(operation: String, lastEntry: Int = 0): List<ProgressEntry> {
        val uuid = UUID.fromString(operation)
        return ProgressEntries.select {
            (ProgressEntries.operation eq uuid) and (ProgressEntries.id greater lastEntry)
        }.orderBy(ProgressEntries.id)
                .map { convertProgressEntry(it) }
    }
}
