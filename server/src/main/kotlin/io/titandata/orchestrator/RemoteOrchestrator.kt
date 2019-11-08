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

    fun listRemotes(repo: String): List<Remote> {
        NameUtil.validateRepoName(repo)
        return transaction {
            providers.metadata.getRepository(repo)
            providers.metadata.listRemotes(repo)
        }
    }

    fun addRemote(repo: String, remote: Remote) {
        NameUtil.validateRepoName(repo)
        NameUtil.validateRemoteName(remote.name)
        transaction {
            providers.metadata.getRepository(repo)
            providers.metadata.addRemote(repo, remote)
        }
    }

    fun getRemote(repo: String, remoteName: String): Remote {
        NameUtil.validateRepoName(repo)
        NameUtil.validateRemoteName(remoteName)
        return transaction {
            providers.metadata.getRepository(repo)
            providers.metadata.getRemote(repo, remoteName)
        }
    }

    fun removeRemote(repo: String, remoteName: String) {
        NameUtil.validateRepoName(repo)
        NameUtil.validateRemoteName(remoteName)
        transaction {
            providers.metadata.getRepository(repo)
            providers.metadata.removeRemote(repo, remoteName)
        }
    }

    fun updateRemote(repo: String, remoteName: String, remote: Remote) {
        NameUtil.validateRepoName(repo)
        NameUtil.validateRemoteName(remoteName)
        NameUtil.validateRemoteName(remote.name)
        transaction {
            providers.metadata.getRepository(repo)
            providers.metadata.updateRemote(repo, remoteName, remote)
        }
    }

    fun listRemoteCommits(repo: String, remoteName: String, params: RemoteParameters, tags: List<String>?): List<Commit> {
        val remote = getRemote(repo, remoteName)
        val commits = providers.remote(remote.provider).listCommits(remote, params, tags)
        return commits.sortedByDescending { OffsetDateTime.parse(it.properties.get("timestamp")?.toString()
                ?: DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(0)),
                DateTimeFormatter.ISO_DATE_TIME) }
    }

    fun getRemoteCommit(repo: String, remoteName: String, params: RemoteParameters, commitId: String): Commit {
        val remote = getRemote(repo, remoteName)
        return providers.remote(remote.provider).getCommit(remote, commitId, params)
    }
}
