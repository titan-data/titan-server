package io.titandata.orchestrator

import io.titandata.ProviderModule
import io.titandata.models.Commit
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
        val volumeSet = transaction {
            providers.metadata.getActiveVolumeSet(name)
        }
        val volumes = transaction {
            providers.metadata.listVolumes(volumeSet)
        }
        val volumeStatus = volumes.map {
            providers.storage.getVolumeStatus(volumeSet, it.name)
        }
        return transaction {
            RepositoryStatus(
                    logicalSize = 0,
                    actualSize = 0,
                    volumeStatus = volumeStatus,
                    lastCommit = providers.metadata.getLastCommit(name),
                    sourceCommit = providers.metadata.getCommitSource(volumeSet)
            )
        }
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
            // TODO mark all commits deleting
            // TODO mark all volumesets deleting
            providers.metadata.deleteRepository(name)
        }
    }
}
