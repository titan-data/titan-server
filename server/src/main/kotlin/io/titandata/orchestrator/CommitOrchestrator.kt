package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.CommitStatus
import org.jetbrains.exposed.sql.transactions.transaction
import java.rmi.NoSuchObjectException
import java.time.Instant
import java.time.format.DateTimeFormatter

class CommitOrchestrator(val providers: ProviderModule) {

    /*
     * To create a new commit, we fetch the active volume set, and then inform the storage provider to create a commit
     * for all volumes within that volume set.
     */
    fun createCommit(repo: String, commit: Commit): Commit {
        NameUtil.validateRepoName(repo)
        NameUtil.validateCommitId(commit.id)

        // Set the creation timestamp in metadata if it doesn't already exists
        @Suppress("UNCHECKED_CAST")
        val tags = (commit.properties.get("tags") as Map<String, String>?)?.toMutableMap() ?: mutableMapOf()
        if (!tags.containsKey("timestamp")) {
            tags["timestamp"] = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
        }
        val properties = commit.properties.toMutableMap()
        properties["tags"] = tags
        val newCommit = Commit(id = commit.id, properties=properties)

        val volumeSet = transaction {
            try {
                providers.metadata.getCommit(repo, commit.id)
                throw ObjectExistsException("commit '${commit.id}' already exists in repository '$repo'")
            } catch (e: NoSuchObjectException) {
                // Ignore
            }
            val vs = providers.metadata.getActiveVolumeSet(repo)
            providers.metadata.createCommit(repo, vs, newCommit)
            vs
        }

        val volumes = transaction {
            providers.metadata.listVolumes(volumeSet)
        }

        providers.storage.createCommit(volumeSet, newCommit.id, volumes.map { it.name })
        return newCommit
    }

    fun getCommit(repo: String, id: String): Commit {
        NameUtil.validateRepoName(repo)
        NameUtil.validateCommitId(id)

        return providers.metadata.getCommit(repo, id).second
    }

    fun getCommitStatus(repo: String, id: String): CommitStatus {
        NameUtil.validateRepoName(repo)
        NameUtil.validateCommitId(id)

        val vs = transaction {
            providers.metadata.getCommit(repo, id).first
        }
        return providers.storage.getCommitStatus(repo, vs, id)
    }

    fun listCommits(repo: String, tags: List<String>?): List<Commit> {
        NameUtil.validateRepoName(repo)

        return providers.metadata.listCommits(repo, tags)
    }

    fun deleteCommit(repo: String, commit: String) {
        NameUtil.validateRepoName(repo)
        NameUtil.validateCommitId(commit)

        val activeVolumeSet = transaction {
            providers.metadata.getActiveVolumeSet(repo)
        }
        val commitVolumeSet = transaction {
            val vs = providers.metadata.getCommit(repo, commit).first
            providers.metadata.deleteCommit(repo, commit)
            vs
        }
        return providers.storage.deleteCommit(repo, activeVolumeSet, commitVolumeSet, commit)
    }

    fun checkoutCommit(repo: String, commit: String) {
        NameUtil.validateRepoName(repo)
        NameUtil.validateCommitId(commit)

        val activeVolumeSet = transaction {
            providers.metadata.getActiveVolumeSet(repo)
        }
        val newVolumeSet = transaction {
            providers.metadata.createVolumeSet(repo)
        }
        val volumes = transaction {
            providers.metadata.listVolumes(activeVolumeSet)
        }
        val commitVolumeSet = transaction {
            providers.metadata.getCommit(repo, commit).first
        }
        providers.storage.checkoutCommit(repo, activeVolumeSet, newVolumeSet, commitVolumeSet, commit)
        transaction {
            for (v in volumes) {
                providers.metadata.createVolume(newVolumeSet, v)
            }
            providers.metadata.activateVolumeSet(repo, newVolumeSet)
            providers.metadata.markVolumeSetDeleting(activeVolumeSet)
        }
    }

    fun updateCommit(repo: String, commit: Commit) {
        NameUtil.validateRepoName(repo)
        NameUtil.validateCommitId(commit.id)

        transaction {
            providers.metadata.updateCommit(repo, commit)
        }
    }
}
