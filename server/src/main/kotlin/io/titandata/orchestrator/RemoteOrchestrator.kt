package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import java.time.Instant
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import org.jetbrains.exposed.sql.transactions.transaction

class RemoteOrchestrator(val providers: ProviderModule) {

    internal fun getTags(tags: List<String>?) : List<Pair<String, String?>> {
        if (tags == null) {
            return emptyList()
        }
        val ret = mutableListOf<Pair<String, String?>>()
        for (tag in tags) {
            if (tag.contains("=")) {
                ret.add(tag.substringBefore("=") to tag.substringAfter("="))
            } else {
                ret.add(tag to null)
            }
        }
        return ret.toList()
    }

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
            validateRemote(providers.metadata.getRemote(repo, remoteName))
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
        val commits = providers.dynamicRemote(remote.provider).listCommits(remote.properties,
                validateParameters(params).properties, getTags(tags))
        return commits.map { Commit(id=it.first, properties=it.second)}
    }

    fun getRemoteCommit(repo: String, remoteName: String, params: RemoteParameters, commitId: String): Commit {
        val remote = getRemote(repo, remoteName)
        val commit = providers.dynamicRemote(remote.provider).getCommit(remote.properties,
                validateParameters(params).properties, commitId)
        if (commit == null) {
            throw NoSuchObjectException("no such commit '$commitId' in remote '$remoteName'")
        }
        return Commit(id= commitId, properties=commit)
    }
}
