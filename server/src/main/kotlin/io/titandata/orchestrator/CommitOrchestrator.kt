package io.titandata.orchestrator

import io.titandata.ServiceLocator
import io.titandata.exception.NoSuchObjectException
import io.titandata.exception.ObjectExistsException
import io.titandata.models.Commit
import io.titandata.models.CommitStatus
import java.time.Instant
import java.time.format.DateTimeFormatter
import org.jetbrains.exposed.sql.transactions.transaction

class CommitOrchestrator(val services: ServiceLocator) {

    /*
     * To create a new commit, we fetch the active volume set, and then inform the storage provider to create a commit
     * for all volumes within that volume set.
     */
    fun createCommit(repo: String, commit: Commit, existingVolumeSet: String? = null): Commit {
        NameUtil.validateCommitId(commit.id)
        services.repositories.getRepository(repo)

        // Set the creation timestamp in metadata if it doesn't already exists
        val props = commit.properties.toMutableMap()
        if (!props.containsKey("timestamp")) {
            props["timestamp"] = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
        }
        val newCommit = Commit(id = commit.id, properties = props)

        val volumeSet = transaction {
            try {
                services.metadata.getCommit(repo, commit.id)
                throw ObjectExistsException("commit '${commit.id}' already exists in repository '$repo'")
            } catch (e: NoSuchObjectException) {
                // Ignore
            }
            val vs = if (existingVolumeSet != null) { existingVolumeSet } else { services.metadata.getActiveVolumeSet(repo) }
            services.metadata.createCommit(repo, vs, newCommit)
            vs
        }

        val volumes = transaction {
            services.metadata.listVolumes(volumeSet)
        }

        services.context.createCommit(volumeSet, newCommit.id, volumes.map { it.name })
        return newCommit
    }

    fun getCommit(repo: String, id: String): Commit {
        NameUtil.validateCommitId(id)
        services.repositories.getRepository(repo)
        return transaction {
            services.metadata.getCommit(repo, id).second
        }
    }

    fun getCommitStatus(repo: String, id: String): CommitStatus {
        NameUtil.validateCommitId(id)
        services.repositories.getRepository(repo)
        val (vs, volumes) = transaction {
            val vs = services.metadata.getCommit(repo, id).first
            Pair(vs, services.metadata.listVolumes(vs).map { it.name })
        }
        return services.context.getCommitStatus(vs, id, volumes)
    }

    fun listCommits(repo: String, tags: List<String>? = null): List<Commit> {
        services.repositories.getRepository(repo)
        return transaction {
            services.metadata.listCommits(repo, tags)
        }
    }

    fun deleteCommit(repo: String, commit: String) {
        NameUtil.validateCommitId(commit)
        services.repositories.getRepository(repo)

        transaction {
            services.metadata.markCommitDeleting(repo, commit)
        }
        services.reaper.signal()
    }

    fun checkoutCommit(repo: String, commit: String) {
        NameUtil.validateCommitId(commit)
        services.repositories.getRepository(repo)
        val (sourceVolumeSet, newVolumeSet) = transaction {
            val vs = services.metadata.getCommit(repo, commit).first
            val newVolumeSet = services.metadata.createVolumeSet(repo, commit)
            val volumes = services.metadata.listVolumes(vs)
            for (v in volumes) {
                services.metadata.createVolume(newVolumeSet, v)
            }
            Pair(vs, newVolumeSet)
        }

        val volumes = transaction {
            services.metadata.listVolumes(newVolumeSet).map { it.name }
        }

        services.context.cloneVolumeSet(sourceVolumeSet, commit, newVolumeSet)
        for (v in volumes) {
            val config = services.context.cloneVolume(sourceVolumeSet, commit, newVolumeSet, v)
            transaction {
                services.metadata.updateVolumeConfig(newVolumeSet, v, config)
            }
        }

        transaction {
            services.metadata.activateVolumeSet(repo, newVolumeSet)
        }
        services.reaper.signal()
    }

    fun updateCommit(repo: String, commit: Commit) {
        NameUtil.validateCommitId(commit.id)
        services.repositories.getRepository(repo)

        transaction {
            services.metadata.updateCommit(repo, commit)
        }
    }
}
