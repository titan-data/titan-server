package io.titandata.orchestrator

import io.titandata.ServiceLocator
import io.titandata.exception.NoSuchObjectException
import io.titandata.models.Commit
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import org.jetbrains.exposed.sql.transactions.transaction

class RemoteOrchestrator(val services: ServiceLocator) {

    internal fun getTags(tags: List<String>?): List<Pair<String, String?>> {
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
                properties = services.remoteProvider(remote.provider).validateRemote(remote.properties)
        )
    }

    fun validateParameters(parameters: RemoteParameters): RemoteParameters {
        return RemoteParameters(
                provider = parameters.provider,
                properties = services.remoteProvider(parameters.provider).validateParameters(parameters.properties)
        )
    }

    fun listRemotes(repo: String): List<Remote> {
        services.repositories.getRepository(repo)
        return transaction {
            services.metadata.listRemotes(repo)
        }
    }

    fun addRemote(repo: String, remote: Remote) {
        NameUtil.validateRemoteName(remote.name)
        services.repositories.getRepository(repo)
        transaction {
            services.metadata.addRemote(repo, validateRemote(remote))
        }
    }

    fun getRemote(repo: String, remoteName: String): Remote {
        NameUtil.validateRemoteName(remoteName)
        services.repositories.getRepository(repo)
        return transaction {
            validateRemote(services.metadata.getRemote(repo, remoteName))
        }
    }

    fun removeRemote(repo: String, remoteName: String) {
        NameUtil.validateRemoteName(remoteName)
        services.repositories.getRepository(repo)
        transaction {
            services.metadata.removeRemote(repo, remoteName)
        }
    }

    fun updateRemote(repo: String, remoteName: String, remote: Remote) {
        NameUtil.validateRemoteName(remoteName)
        NameUtil.validateRemoteName(remote.name)
        services.repositories.getRepository(repo)
        transaction {
            services.metadata.updateRemote(repo, remoteName, validateRemote(remote))
        }
    }

    fun listRemoteCommits(repo: String, remoteName: String, params: RemoteParameters, tags: List<String>?): List<Commit> {
        val remote = getRemote(repo, remoteName)
        if (params.provider != remote.provider) {
            throw IllegalArgumentException("invalid remote parameter type '${params.provider}' for remote type '${remote.provider}")
        }
        val commits = services.remoteProvider(remote.provider).listCommits(remote.properties,
                validateParameters(params).properties, getTags(tags))
        return commits.map { Commit(id = it.first, properties = it.second) }
    }

    fun getRemoteCommit(repo: String, remoteName: String, params: RemoteParameters, commitId: String): Commit {
        val remote = getRemote(repo, remoteName)
        if (params.provider != remote.provider) {
            throw IllegalArgumentException("invalid remote parameter type '${params.provider}' for remote type '${remote.provider}")
        }
        val commit = services.remoteProvider(remote.provider).getCommit(remote.properties,
                validateParameters(params).properties, commitId)
        if (commit == null) {
            throw NoSuchObjectException("no such commit '$commitId' in remote '$remoteName'")
        }
        return Commit(id = commitId, properties = commit)
    }
}
