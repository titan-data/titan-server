package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.Repository
import io.titandata.models.RepositoryStatus
import org.jetbrains.exposed.sql.transactions.transaction

class RepositoryOrchestrator(val providers: ProviderModule) {

    fun createRepository(repo: Repository) {
        NameUtil.validateRepoName(repo.name)
        val volumeSet = transaction {
            providers.metadata.createRepository(repo)
            val vs = providers.metadata.createVolumeSet(repo.name, null, true)
            vs
        }
        providers.storage.createVolumeSet(volumeSet)
    }

    fun listRepositories(): List<Repository> {
        return transaction {
            providers.metadata.listRepositories()
        }
    }

    fun getRepository(name: String): Repository {
        NameUtil.validateRepoName(name)
        return transaction {
            providers.metadata.getRepository(name)
        }
    }

    fun getRepositoryStatus(name: String): RepositoryStatus {
        NameUtil.validateRepoName(name)
        val (volumeSet, volumes) = transaction {
            val vs = providers.metadata.getActiveVolumeSet(name)
            Pair(vs, providers.metadata.listVolumes(vs))
        }
        val volumeStatus = volumes.map {
            providers.storage.getVolumeStatus(volumeSet, it.name)
        }
        val status = transaction {
            RepositoryStatus(
                    volumeStatus = volumeStatus,
                    lastCommit = providers.metadata.getLastCommit(name),
                    sourceCommit = providers.metadata.getCommitSource(volumeSet)
            )
        }
        return status
    }

    fun updateRepository(name: String, repo: Repository) {
        NameUtil.validateRepoName(name)
        NameUtil.validateRepoName(repo.name)
        transaction {
            providers.metadata.updateRepository(name, repo)
        }
    }

    fun deleteRepository(name: String) {
        NameUtil.validateRepoName(name)
        transaction {
            providers.metadata.markAllVolumeSetsDeleting(name)
            providers.metadata.deleteRepository(name)
        }
        providers.reaper.signal()
    }
}
