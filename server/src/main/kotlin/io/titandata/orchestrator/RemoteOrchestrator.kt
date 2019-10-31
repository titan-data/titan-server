package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.Remote
import io.titandata.models.RemoteParameters
import java.time.Instant
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

class RemoteOrchestrator(val providers: ProviderModule) {

    private fun findRemote(repo: String, remote: String): Pair<MutableList<Remote>, Int?> {
        val remotes = providers.storage.getRemotes(repo).toMutableList()
        remotes.forEachIndexed { idx, r ->
            if (r.name == remote) {
                return Pair(remotes, idx)
            }
        }
        return Pair(remotes, null)
    }

    fun listRemotes(repo: String): List<Remote> {
        return providers.storage.getRemotes(repo)
    }

    fun addRemote(repo: String, remote: Remote) {
        val (remotes, idx) = findRemote(repo, remote.name)
        if (idx != null) {
            throw ObjectExistsException("remote '${remote.name}' exists for repository '$repo'")
        }
        remotes.add(remote)
        providers.storage.updateRemotes(repo, remotes)
    }

    fun getRemote(repo: String, remoteName: String): Remote {
        val (remotes, idx) = findRemote(repo, remoteName)
        if (idx == null) {
            throw NoSuchObjectException("no such remote '$remoteName' in repository '$repo'")
        }
        return remotes[idx]
    }

    fun removeRemote(repo: String, remoteName: String) {
        val (remotes, idx) = findRemote(repo, remoteName)
        if (idx == null) {
            throw NoSuchObjectException("no such remote '$remoteName' in repository '$repo'")
        }

        for (op in providers.operations.listOperations(repo)) {
            if (op.remote == remoteName) {
                providers.operations.abortOperation(repo, op.id)
            }
        }

        remotes.removeAt(idx)
        providers.storage.updateRemotes(repo, remotes)
    }

    fun updateRemote(repo: String, remoteName: String, remote: Remote) {
        val (remotes, idx) = findRemote(repo, remoteName)
        if (idx == null) {
            throw NoSuchObjectException("no such remote '$remoteName' in repository '$repo'")
        }
        if (remoteName != remote.name) {
            val (_, newIdx) = findRemote(repo, remote.name)
            if (newIdx != null) {
                throw ObjectExistsException("remote '$remoteName' already exists for repository '$repo'")
            }
        }
        remotes[idx] = remote
        providers.storage.updateRemotes(repo, remotes)
    }

    fun listRemoteCommits(repo: String, remoteName: String, params: RemoteParameters, tags: List<String>?): List<Commit> {
        val (remotes, idx) = findRemote(repo, remoteName)
        if (idx == null) {
            throw NoSuchObjectException("no such remote '$remoteName' in repository '$repo'")
        }
        val remote = remotes[idx]
        val commits = providers.remote(remote.provider).listCommits(remote, params, tags)
        return commits.sortedByDescending { OffsetDateTime.parse(it.properties.get("timestamp")?.toString()
                ?: DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(0)),
                DateTimeFormatter.ISO_DATE_TIME) }
    }

    fun getRemoteCommit(repo: String, remoteName: String, params: RemoteParameters, commitId: String): Commit {
        val (remotes, idx) = findRemote(repo, remoteName)
        if (idx == null) {
            throw NoSuchObjectException("no such remote '$remoteName' in repository '$repo'")
        }
        val remote = remotes[idx]
        return providers.remote(remote.provider).getCommit(remote, commitId, params)
    }
}
