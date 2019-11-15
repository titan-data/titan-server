package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.Commit
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import java.time.Instant
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import org.jetbrains.exposed.sql.transactions.transaction

class RemoteOrchestrator(val providers: ProviderModule) {

    fun validateRemote(remote: Remote): Remote {
        return Remote(
                provider = remote.provider,
                name = remote.name,
                properties = providers.dynamicRemote(remote.provider).validateRemote(remote.properties)
        )
    }

    fun validateParameters(parameters: RemoteParameters): RemoteParameters {
        return RemoteParameters(
                provider = parameters.provider,
                properties = providers.dynamicRemote(parameters.provider).validateParameters(parameters.properties)
        )
    }

    fun listRemotes(repo: String): List<Remote> {
        providers.repositories.getRepository(repo)
        return transaction {
            providers.metadata.listRemotes(repo)
        }
    }

    fun addRemote(repo: String, remote: Remote) {
        NameUtil.validateRemoteName(remote.name)
        providers.repositories.getRepository(repo)
        transaction {
            providers.metadata.addRemote(repo, validateRemote(remote))
        }
    }

    fun getRemote(repo: String, remoteName: String): Remote {
        NameUtil.validateRemoteName(remoteName)
        providers.repositories.getRepository(repo)
        return transaction {
            providers.metadata.getRemote(repo, remoteName)
        }
    }

    fun removeRemote(repo: String, remoteName: String) {
        NameUtil.validateRemoteName(remoteName)
        providers.repositories.getRepository(repo)
        transaction {
            providers.metadata.removeRemote(repo, remoteName)
        }
    }

    fun updateRemote(repo: String, remoteName: String, remote: Remote) {
        NameUtil.validateRemoteName(remoteName)
        NameUtil.validateRemoteName(remote.name)
        providers.repositories.getRepository(repo)
        transaction {
            providers.metadata.updateRemote(repo, remoteName, validateRemote(remote))
        }
    }

    fun listRemoteCommits(repo: String, remoteName: String, params: RemoteParameters, tags: List<String>?): List<Commit> {
        val remote = getRemote(repo, remoteName)
        val commits = providers.remote(remote.provider).listCommits(remote, validateParameters(params), tags)
        return commits.sortedByDescending { OffsetDateTime.parse(it.properties.get("timestamp")?.toString()
                ?: DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(0)),
                DateTimeFormatter.ISO_DATE_TIME) }
    }

    fun getRemoteCommit(repo: String, remoteName: String, params: RemoteParameters, commitId: String): Commit {
        val remote = getRemote(repo, remoteName)
        return providers.remote(remote.provider).getCommit(remote, commitId, validateParameters(params))
    }
}
