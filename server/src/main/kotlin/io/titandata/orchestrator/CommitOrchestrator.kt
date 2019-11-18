package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.CommitStatus
import java.time.Instant
import java.time.format.DateTimeFormatter
import org.jetbrains.exposed.sql.transactions.transaction

class CommitOrchestrator(val providers: ProviderModule) {

    /*
     * To create a new commit, we fetch the active volume set, and then inform the storage provider to create a commit
     * for all volumes within that volume set.
     */
    fun createCommit(repo: String, commit: Commit, existingVolumeSet: String? = null): Commit {
        NameUtil.validateCommitId(commit.id)
        providers.repositories.getRepository(repo)

        // Set the creation timestamp in metadata if it doesn't already exists
        val props = commit.properties.toMutableMap()
        if (!props.containsKey("timestamp")) {
            props["timestamp"] = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
        }
        val newCommit = Commit(id = commit.id, properties = props)

        val volumeSet = transaction {
            try {
                providers.metadata.getCommit(repo, commit.id)
                throw ObjectExistsException("commit '${commit.id}' already exists in repository '$repo'")
            } catch (e: NoSuchObjectException) {
                // Ignore
            }
            val vs = if (existingVolumeSet != null) { existingVolumeSet } else { providers.metadata.getActiveVolumeSet(repo) }
            providers.metadata.createCommit(repo, vs, newCommit)
            vs
        }

        val volumes = transaction {
            providers.metadata.listVolumes(volumeSet)
        }

        providers.context.createCommit(volumeSet, newCommit.id, volumes.map { it.name })
        return newCommit
    }

    fun getCommit(repo: String, id: String): Commit {
        NameUtil.validateCommitId(id)
        providers.repositories.getRepository(repo)
        return transaction {
            providers.metadata.getCommit(repo, id).second
        }
    }

    fun getCommitStatus(repo: String, id: String): CommitStatus {
        NameUtil.validateCommitId(id)
        providers.repositories.getRepository(repo)
        val (vs, volumes) = transaction {
            val vs = providers.metadata.getCommit(repo, id).first
            Pair(vs, providers.metadata.listVolumes(vs).map { it.name })
        }
        return providers.context.getCommitStatus(vs, id, volumes)
    }

    fun listCommits(repo: String, tags: List<String>? = null): List<Commit> {
        providers.repositories.getRepository(repo)
        return transaction {
            providers.metadata.listCommits(repo, tags)
        }
    }

    fun deleteCommit(repo: String, commit: String) {
        NameUtil.validateCommitId(commit)
        providers.repositories.getRepository(repo)

        transaction {
            providers.metadata.markCommitDeleting(repo, commit)
        }
        providers.reaper.signal()
    }

    fun checkoutCommit(repo: String, commit: String) {
        NameUtil.validateCommitId(commit)
        providers.repositories.getRepository(repo)
        val (sourceVolumeSet, newVolumeSet) = transaction {
            val vs = providers.metadata.getCommit(repo, commit).first
            val newVolumeSet = providers.metadata.createVolumeSet(repo, commit)
            val volumes = providers.metadata.listVolumes(vs)
            for (v in volumes) {
                providers.metadata.createVolume(newVolumeSet, v)
            }
            Pair(vs, newVolumeSet)
        }

        val volumes = transaction {
            providers.metadata.listVolumes(newVolumeSet).map { it.name }
        }

        providers.context.cloneVolumeSet(sourceVolumeSet, commit, newVolumeSet)
        for (v in volumes) {
            val config = providers.context.cloneVolume(sourceVolumeSet, commit, newVolumeSet, v)
            transaction {
                providers.metadata.updateVolumeConfig(newVolumeSet, v, config)
            }
        }

        transaction {
            providers.metadata.activateVolumeSet(repo, newVolumeSet)
        }
        providers.reaper.signal()
    }

    fun updateCommit(repo: String, commit: Commit) {
        NameUtil.validateCommitId(commit.id)
        providers.repositories.getRepository(repo)

        transaction {
            providers.metadata.updateCommit(repo, commit)
        }
    }
}
